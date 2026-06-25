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
    config_file = 'zuul-mqtt-trigger.conf'
    tenant_config_file = 'config/mqtt-trigger/main.yaml'

    def setUp(self):
        from tests import fake_mqtt
        mqtt_fixture = fake_mqtt.MQTTFixture()
        self.broker = mqtt_fixture.broker
        self.useFixture(mqtt_fixture)
        super().setUp()

    def test_mqtt_triggers_build_on_branch_ref(self):
        """Test MQTT Triggers Build on Branch Ref"""
        topic = 'tenant-one/zuul/testevent'
        event = {
            'tenant': 'tenant-one',
            'project_canonical_name': 'review.example.com/org/project',
            'ref': 'refs/heads/master',
            'action': 'enqueue'
        }
        self.executor_server.hold_jobs_in_build = True
        # Trigger a job through MQTT
        self.broker.publish(topic, json.dumps(event), 2)
        self.waitUntilSettled()
        self.assertTrue(len(self.builds) > 0)
        # Ensure that the correct project enqueued into the correct pipeline
        self.assertEqual(self.builds[0].project, 'org/project')
        self.assertEqual(self.builds[0].pipeline, 'check')

        # Let the job complete and check the result
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        self.assertTrue(len(self.history) > 0)
        self.assertHistory([
            dict(name='project-test1', result='SUCCESS',
                 ref='refs/heads/master'),
        ])

    def test_mqtt_triggers_build_on_branch_mapped_to_ref(self):
        """Test MQTT Triggers Build on Branch Mapped to a Ref"""
        topic = 'tenant-one/zuul/testevent'
        event = {
            'tenant': 'tenant-one',
            'project_canonical_name': 'review.example.com/org/project',
            'branch': 'master',
            'action': 'enqueue'
        }
        self.executor_server.hold_jobs_in_build = True
        # Trigger a job through MQTT
        self.broker.publish(topic, json.dumps(event), 2)
        self.waitUntilSettled()
        self.assertTrue(len(self.builds) > 0)
        # Ensure that the correct project enqueued into the correct pipeline
        self.assertEqual(self.builds[0].project, 'org/project')
        self.assertEqual(self.builds[0].pipeline, 'check')

        # Let the job complete and check the result
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        self.assertTrue(len(self.history) > 0)
        self.assertHistory([
            dict(name='project-test1', result='SUCCESS',
                 ref='refs/heads/master'),
        ])

    def test_mqtt_triggers_build_on_change(self):
        """Test MQTT Triggers Build on Change"""
        self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        topic = 'tenant-one/zuul/testevent'
        event = {
            'tenant': 'tenant-one',
            'project_hostname': 'review.example.com',
            'project_name': 'org/project',
            'change': 1,
            'action': 'enqueue'
        }
        self.executor_server.hold_jobs_in_build = True
        # Trigger a job through MQTT
        self.broker.publish(topic, json.dumps(event), 2)
        self.waitUntilSettled()
        self.assertTrue(len(self.builds) == 1)
        # Ensure that the correct project enqueued into the correct pipeline
        self.assertEqual(self.builds[0].project, 'org/project')
        self.assertEqual(self.builds[0].pipeline, 'check')

        # Let the job complete and check the result
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        self.assertTrue(len(self.history) == 1)
        self.assertHistory([
            dict(name='project-test1', result='SUCCESS',
                 changes='1,1', ref='refs/changes/01/1/1'),
        ])

        # Now enqueue the same change with an explicit patchset.
        # Zuul won't enqueue the same change twice so we have to process the
        # change above before proceeding here.
        self.executor_server.hold_jobs_in_build = True
        event['patchset'] = 1
        self.broker.publish(topic, json.dumps(event), 2)
        self.waitUntilSettled()
        self.assertTrue(len(self.builds) == 1)
        # Ensure that the correct project enqueued into the correct pipeline
        self.assertEqual(self.builds[0].project, 'org/project')
        self.assertEqual(self.builds[0].pipeline, 'check')

        # Let the job complete and check the result
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        self.assertTrue(len(self.history) == 2)
        self.assertHistory([
            dict(name='project-test1', result='SUCCESS',
                 changes='1,1', ref='refs/changes/01/1/1'),
            dict(name='project-test1', result='SUCCESS',
                 changes='1,1', ref='refs/changes/01/1/1'),
        ])

    def test_mqtt_triggers_ignore_non_matching_events(self):
        """Test non matching MQTT events don't trigger builds"""
        self.executor_server.hold_jobs_in_build = True
        # This topic doesn't match the trigger configuration topic
        topic = 'tenant-one/zuul/testing'
        event = {
            'tenant': 'tenant-one',
            'project_canonical_name': 'review.example.com/org/project',
            'ref': 'master',
            'action': 'enqueue'
        }
        self.broker.publish(topic, json.dumps(event), 2)
        self.waitUntilSettled()
        self.assertTrue(len(self.builds) == 0)

        # This action does not match the trigger configuration action
        event['action'] = 'dequeue'
        topic = 'tenant-one/zuul/testevent'
        self.broker.publish(topic, json.dumps(event), 2)
        self.waitUntilSettled()
        self.assertTrue(len(self.builds) == 0)

    def test_mqtt_triggers_ignore_events_with_unmapped_data(self):
        """Test that MQTT events with unmapped payload data are ignored"""
        self.executor_server.hold_jobs_in_build = True
        topic = 'tenant-one/zuul/testevent'
        # The event payload is missing project source info and will be ignored
        event = {
            'tenant': 'tenant-one',
            'ref': 'master',
            'action': 'enqueue'
        }
        self.broker.publish(topic, json.dumps(event), 2)
        self.waitUntilSettled()
        self.assertTrue(len(self.builds) == 0)

        # Now set the project source info but remove the ref value. This
        # should also be ignored.
        event['project_canonical_name'] = 'review.example.com/org/project'
        del event['ref']
        self.broker.publish(topic, json.dumps(event), 2)
        self.waitUntilSettled()
        self.assertTrue(len(self.builds) == 0)
