# Copyright 2023 Acme Gating, LLC
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

import json
import google.api_core.exceptions
from google.oauth2 import service_account
from google.cloud import pubsub_v1
import logging
import pprint
import threading


class GerritGcloudPubsubEventListener:
    log = logging.getLogger("zuul.GerritConnection.gcloudpubsub")
    RECONNECTION_DELAY = 5

    def __init__(self, gerrit_connection, connection_config):
        self.gerrit_connection = gerrit_connection
        project = connection_config.get('gcloud_pubsub_project')
        topic = connection_config.get('gcloud_pubsub_topic', 'gerrit')
        sub = connection_config.get('gcloud_pubsub_subscription_id', 'zuul')
        key = connection_config.get('gcloud_pubsub_private_key')
        self.kwargs = {}
        if key:
            with open(key) as keyfile:
                info = json.load(keyfile)
                credentials = service_account.Credentials.\
                    from_service_account_info(info)
                self.kwargs['credentials'] = credentials
        self.topic_name = f'projects/{project}/topics/{topic}'
        self.subscription_name = f'projects/{project}/subscriptions/{sub}'
        self._stop_event = threading.Event()
        self._stopped = False
        self._thread = None
        self._future = None

    def start(self):
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()

    def stop(self):
        self.log.debug("Stopping gcloud pubsub listener")
        self._stopped = True
        self._stop_event.set()
        try:
            if self._future:
                self._future.cancel()
        except Exception:
            self.log.exception("Error canceling future:")

    def join(self):
        if self._thread:
            self._thread.join()

    def callback(self, message):
        data = json.loads(message.data)
        self.log.info("Received data from gcloud: \n%s" %
                      pprint.pformat(data))
        self.gerrit_connection.addEvent(data)
        message.ack()

    def _run(self):
        subscriber = pubsub_v1.SubscriberClient(**self.kwargs)
        # So the unit tests can access it
        self.subscriber = subscriber
        with subscriber as client:
            try:
                client.create_subscription(name=self.subscription_name,
                                           topic=self.topic_name)
            except google.api_core.exceptions.AlreadyExists:
                pass
            self._future = client.subscribe(
                self.subscription_name, self.callback)
            self._future.result()

    def run(self):
        while not self._stopped:
            try:
                self._run()
            except Exception:
                self.log.exception(
                    "Exception in gcloud pubsub consumer with %s:",
                    self.gerrit_connection.connection_name)
                self._stop_event.wait(self.RECONNECTION_DELAY)
