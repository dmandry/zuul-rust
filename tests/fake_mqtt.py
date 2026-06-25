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

import fixtures
from paho.mqtt.client import topic_matches_sub


class FakeMQTTBroker:
    log = logging.getLogger('zuul.FakeMQTTBroker')

    def __init__(self):
        # Client -> subscribed topics
        self.clients = {}
        self.messages = []

    def registerClient(self, client):
        self.clients[client] = set()

    def unregisterClient(self, client):
        self.clients.pop(client)

    def publish(self, topic, payload, qos):
        self.log.info('Publishing message via mqtt %s %s', topic, payload)
        # The tests expect json payloads for historical reasons; this
        # could be changed.
        msg = json.loads(payload)
        self.messages.append({'topic': topic, 'msg': msg, 'qos': qos})
        for client, topics in self.clients.items():
            for client_sub in topics:
                if topic_matches_sub(client_sub, topic):
                    if f := getattr(client, 'on_message', None):
                        # TODO Need to convert payload to a paho mqtt message
                        # with payload and topic attributes then pass it here.
                        f(self, None, None)
                    break


class FakeMQTTService:
    _fake_data = {}

    def __init__(self, client_id):
        pass

    def connect(self, server, port, keepalive):
        self.broker.registerClient(self)

    def loop_start(self):
        pass

    def loop_stop(self):
        pass

    def disconnect(self):
        self.broker.unregisterClient(self)
        if f := getattr(self, 'on_disconnect', None):
            f(self, None, None)

    def subscribe(self, topic, qos):
        self.broker.clients[self].add(topic)

    def unsubscribe(self, topic):
        self.broker.clients[self].discard(topic)

    def publish(self, topic, payload, qos):
        self.broker.publish(topic, str(payload), qos)


class MQTTFixture(fixtures.Fixture):
    def __init__(self):
        super().__init__()
        self.broker = FakeMQTTBroker()

    def _setUp(self):
        # Create a new universe shared among any clients we create
        class Client(FakeMQTTService):
            broker = self.broker

        self.useFixture(fixtures.MonkeyPatch(
            'paho.mqtt.client.Client',
            Client))
