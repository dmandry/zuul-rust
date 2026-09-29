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
import confluent_kafka as kafka
import logging
import pprint
import threading

# With multiple Kafka partitions, events could arrive out of order.
# Similar to webhooks, we accept that and don't do anything to
# mitigate that.  They should mostly arrive in the correct order, at
# least at human scale.  That is, while we are somewhat likely to see
# an update to a change and the corresponding update to refs/meta out
# of order, we are unlikely to see a change abandoned before it is
# created.


class GerritKafkaEventListener:
    log = logging.getLogger("zuul.GerritConnection.kafka")
    RECONNECTION_DELAY = 5

    def __init__(self, gerrit_connection, connection_config):
        self.gerrit_connection = gerrit_connection
        bs = connection_config.get('kafka_bootstrap_servers')
        kafka_config = {
            'bootstrap.servers': bs,
        }
        kafka_config['client.id'] = connection_config.get(
            'kafka_client_id', 'zuul')
        kafka_config['group.id'] = connection_config.get(
            'kafka_group_id', 'zuul')
        tls_key = connection_config.get('kafka_tls_key', None)
        tls_cert = connection_config.get('kafka_tls_cert', None)
        tls_ca = connection_config.get('kafka_tls_ca', None)
        if tls_key:
            kafka_config['ssl.key.location'] = tls_key
            kafka_config['ssl.certificate.location'] = tls_cert
            kafka_config['ssl.ca.location'] = tls_ca
        self.kafka_config = kafka_config
        self.topic = connection_config.get('kafka_topic', 'gerrit')
        self._stop_event = threading.Event()
        self._stopped = False
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()

    def stop(self):
        self.log.debug("Stopping kafka listener")
        self._stopped = True
        self._stop_event.set()

    def join(self):
        if self._thread:
            self._thread.join()

    def _run(self):
        self.log.info("Connecting to kafka at %s",
                      self.kafka_config['bootstrap.servers'])
        consumer = kafka.Consumer(self.kafka_config, logger=self.log)
        # So the unit tests can access it
        self.consumer = consumer
        try:
            consumer.subscribe([self.topic])
            while not self._stopped:
                msg = consumer.poll(timeout=2.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == kafka.KafkaError._PARTITION_EOF:
                        self.log.info(
                            "Kafka topic %s partition %s "
                            "reached end at offset %s",
                            msg.topic(), msg.partition(), msg.offset())
                    else:
                        raise kafka.KafkaException(msg.error())
                else:
                    data = json.loads(msg.value().decode('utf8'))
                    self.log.info("Received data from kafka: \n%s" %
                                  pprint.pformat(data))
                    self.gerrit_connection.addEvent(data)
        finally:
            consumer.close()

    def run(self):
        while not self._stopped:
            try:
                self._run()
            except Exception:
                self.log.exception("Exception in kafka consumer with %s:",
                                   self.gerrit_connection.connection_name)
                self._stop_event.wait(self.RECONNECTION_DELAY)
