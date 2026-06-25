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

import json

from tests.base import ZuulTestCase


class TestMQTTTrigger(ZuulTestCase):
    config_file = 'zuul-mqtt-driver.conf'
    tenant_config_file = 'config/mqtt-trigger/main.yaml'

    def setUp(self):
        from tests import fake_mqtt
        mqtt_fixture = fake_mqtt.MQTTFixture()
        self.broker = mqtt_fixture.broker
        self.useFixture(mqtt_fixture)
        super().setUp()

    def test_mqtt_triggers_build(self):
        """Test MQTT Triggers Build"""
        topic = 'tenant-one/zuul/testevent'
        files = {'test_file': 'test_content'}
        ref = self.create_commit('org/project', files=files, head='master',
                                 message='test commit for testing')
        event = {
            'tenant': 'tenant-one',
            'project': 'org/project',
            'branch': 'master',
            'ref': ref,
            'action': 'enqueue'
        }
        self.executor_server.hold_jobs_in_build = True
        self.broker.publish(topic, json.dumps(event), 2)
        self.waitUntilSettled()
        self.assertTrue(len(self.builds) > 0)
        self.executor_server.hold_jobs_in_build = False
        self.waitUntilSettled()
        self.assertTrue(len(self.history) > 0)
