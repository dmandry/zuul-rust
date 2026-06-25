# Copyright 2017 Red Hat, Inc.
# Copyright 2026 Acme Gating, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import logging
import json
import threading
import time

import paho.mqtt.client as mqtt

from zuul.connection import BaseConnection
from zuul.driver.mqtt.mqttmodel import MQTTTriggerEvent
from zuul.exceptions import ConfigurationError
from zuul.lib.logutil import get_annotated_logger
from zuul.zk.event_queues import EventReceiverElection


class MQTTListener(threading.Thread):
    log = logging.getLogger("zuul.connection.mqtt.listener")

    def __init__(self, connection, client, callback, election_name="listener"):
        threading.Thread.__init__(self)
        self.daemon = True
        self.connection = connection
        self.client = client
        self._stopped = False
        self._stop_event = threading.Event()
        self.callback = callback
        self.listener_election = EventReceiverElection(
            connection.sched.zk_client,
            connection.connection_name,
            election_name)
        self.connected = False
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_subscribe = self._on_subscribe
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        self.log.debug("MQTT client connection established")
        self.connected = True

    def _on_disconnect(self, client, userdata, rc):
        self.log.debug("MQTT client disconnected")
        self.connected = False
        self._stop_event.set()

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        # QOS of 128 indicates a subscription error in MQTT 3.x
        if 128 in granted_qos:
            self.log.error("Failed to subscribe to '+/zuul/#' mqtt topic")
        else:
            self.log.debug("Subscribed to mqtt topic")

    def _on_message(self, client, userdata, message):
        if self.listener_election.is_still_valid():
            payload = json.loads(message.payload)
            self.addEvent(message.topic, payload)
        else:
            self._stop_event.set()

    def _run(self):
        self._stop_event.clear()
        while not self._stop_event.is_set():
            # TODO(clarkb) make this topic subscription configurable
            # This matches the events that Zuul itself will publish to
            # the mqtt broker.
            self.log.debug("Subscribing to mqtt topic")
            self.client.subscribe('+/zuul/#', 2)
            self._stop_event.wait()
            if self.connected:
                # Handle the case where we lost the election and set the
                # stop event. In that case we remain connected for
                # publishing but unsubscribe from events.
                self.client.unsubscribe('+/zuul/#')

    def run(self):
        while not self._stopped:
            try:
                self.log.debug("Starting Listener")
                connection_config = self.connection.connection_config
                self.client.connect(
                    connection_config.get('server', 'localhost'),
                    port=int(connection_config.get('port', 1883)),
                    keepalive=int(connection_config.get('keepalive', 60))
                )
                self.connected = True
                self.client.loop_start()
                self.listener_election.run(self._run)
            except Exception:
                self.log.exception("Unexpected issue in _run loop:")

    def stop(self):
        self.log.debug("Stopping Listener")
        self._stopped = True
        self.client.disconnect()
        self.listener_election.cancel()


class MQTTConnection(BaseConnection):
    driver_name = 'mqtt'
    log = logging.getLogger("zuul.MQTTConnection")

    def __init__(self, driver, connection_name, connection_config):
        super(MQTTConnection, self).__init__(driver, connection_name,
                                             connection_config)
        self.client = mqtt.Client(
            client_id=self.connection_config.get('client_id'))
        if self.connection_config.get('user'):
            self.client.username_pw_set(
                self.connection_config.get('user'),
                self.connection_config.get('password'))
        ca_certs = self.connection_config.get('ca_certs')
        certfile = self.connection_config.get('certfile')
        keyfile = self.connection_config.get('keyfile')
        ciphers = self.connection_config.get('ciphers')
        if (ciphers or certfile or keyfile) and not ca_certs:
            raise ConfigurationError(
                "MQTT TLS configuration requires the ca_certs option")
        if ca_certs:
            if bool(certfile) != bool(keyfile):
                raise ConfigurationError(
                    "MQTT configuration keyfile and certfile "
                    "options must both be set.")
            self.client.tls_set(
                ca_certs,
                certfile=certfile,
                keyfile=keyfile,
                ciphers=ciphers)
        # TODO(clarkb) source is used by other connections to refresh
        # caches before handing work to the scheduler. MQTT isn't an
        # actual change source just an event stream. What do we do there?
        # self.source = driver.getSource(self)

    def addEvent(self, topic, payload):
        event = MQTTTriggerEvent()
        event.connection_name = self.connection_name
        event.type = 'ref-updated'
        event.timestamp = time.time()
        event.topic = topic
        for attr in ('ref', 'branch', 'action',
                     'change', 'patchset',
                     'tenant', 'project', 'pipeline'):
            if payload.get(attr):
                setattr(event, attr, payload[attr])

        # Force changes cache update before passing
        # the event to the scheduler
        # change_key = self.source.getChangeKey(event)
        # self.getChange(change_key)
        self.logEvent(event)
        # Pass the event to the scheduler
        self.sched.addTriggerEvent(self.driver_name, event)

    def onLoad(self, zk_client, component_registry):
        # If the connection was not loaded by a scheduler, but by e.g.
        # zuul-web, we want to stop here.
        if not self.sched:
            return

        self.log.debug("Starting MQTT Connection")
        self._start_listener_thread()

    def _start_listener_thread(self):
        self.listener_thread = MQTTListener(self, self.client, self.addEvent)
        self.listener_thread.start()

    def onStop(self):
        self._stop_listener_thread()

    def _stop_listener_thread(self):
        if self.listener_thread:
            self.listener_thread.stop()
            self.listener_thread.join()

    def publish(self, topic, message, qos, zuul_event_id):
        log = get_annotated_logger(self.log, zuul_event_id)
        if not self.listener_thread or not self.listener_thread.connected:
            log.warning("MQTT reporter (%s) is disabled", self)
            return
        try:
            # TODO(clarkb) Does this need to be election aware? It wasn't
            # previously so as long as the client is connected I think we
            # preserve the old behavior.
            self.client.publish(topic, payload=json.dumps(message), qos=qos)
        except Exception:
            log.exception(
                "Could not publish message to topic '%s' via mqtt", topic)
