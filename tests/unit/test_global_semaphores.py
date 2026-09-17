# Copyright 2022 Acme Gating, LLC
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

import zuul.configloader

from tests.base import ZuulTestCase


class TestGlobalSemaphoresConfig(ZuulTestCase):
    tenant_config_file = 'config/global-semaphores-config/main.yaml'

    def assertSemaphores(self, tenant, semaphores):
        for k, v in semaphores.items():
            self.assertEqual(
                len(tenant.semaphore_handler.semaphoreHolders(k)),
                v, k)

    def assertSemaphoresMax(self, tenant, semaphores):
        for k, v in semaphores.items():
            abide = tenant.semaphore_handler.abide
            semaphore = tenant.layout.getSemaphore(abide, k)
            self.assertEqual(semaphore.max, v, k)

    def test_semaphore_scope(self):
        # This tests global and tenant semaphore scope
        self.executor_server.hold_jobs_in_build = True
        tenant1 = self.scheds.first.sched.abide.tenants.get('tenant-one')
        tenant2 = self.scheds.first.sched.abide.tenants.get('tenant-two')
        tenant3 = self.scheds.first.sched.abide.tenants.get('tenant-three')

        # The different max values will tell us that we have the right
        # semaphore objects.  Each tenant has one tenant-scope
        # semaphore in a tenant-specific project, and one tenant-scope
        # semaphore with a common definition.  Tenants 1 and 2 share a
        # global-scope semaphore, and tenant 3 has a tenant-scope
        # semaphore with the same name.

        # Here is what is defined in each tenant:
        # Tenant-one:
        #  * global-semaphore:   scope:global max:100 definition:main.yaml
        #  * common-semaphore:   scope:tenant max:10  definition:common-config
        #  * project1-semaphore: scope:tenant max:11  definition:project1
        #  * (global-semaphore): scope:tenant max:2   definition:project1
        #    [unused since it shadows the actual global-semaphore]
        # Tenant-two:
        #  * global-semaphore:   scope:global max:100 definition:main.yaml
        #  * common-semaphore:   scope:tenant max:10  definition:common-config
        #  * project2-semaphore: scope:tenant max:12  definition:project2
        # Tenant-three:
        #  * global-semaphore:   scope:global max:999 definition:project3
        #  * common-semaphore:   scope:tenant max:10  definition:common-config
        #  * project3-semaphore: scope:tenant max:13  definition:project3
        self.assertSemaphoresMax(tenant1, {'global-semaphore': 100,
                                           'common-semaphore': 10,
                                           'project1-semaphore': 11,
                                           'project2-semaphore': 1,
                                           'project3-semaphore': 1})
        self.assertSemaphoresMax(tenant2, {'global-semaphore': 100,
                                           'common-semaphore': 10,
                                           'project1-semaphore': 1,
                                           'project2-semaphore': 12,
                                           'project3-semaphore': 1})
        # This "global" semaphore is really tenant-scoped, it just has
        # the same name.
        self.assertSemaphoresMax(tenant3, {'global-semaphore': 999,
                                           'common-semaphore': 10,
                                           'project1-semaphore': 1,
                                           'project2-semaphore': 1,
                                           'project3-semaphore': 13})

        # We should have a config error in tenant1 due to the
        # redefinition.
        self.assertEqual(len(tenant1.layout.loading_errors), 1)
        self.assertEqual(len(tenant2.layout.loading_errors), 0)
        self.assertEqual(len(tenant3.layout.loading_errors), 0)

        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        C = self.fake_gerrit.addFakeChange('org/project3', 'master', 'C')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Checking the number of holders tells us whethere we are
        # using global or tenant-scoped semaphores.  Each in-use
        # semaphore in a tenant should have only one holder except the
        # global-scope semaphore shared between tenants 1 and 2.
        self.assertSemaphores(tenant1, {'global-semaphore': 2,
                                        'common-semaphore': 1,
                                        'project1-semaphore': 1,
                                        'project2-semaphore': 0,
                                        'project3-semaphore': 0})
        self.assertSemaphores(tenant2, {'global-semaphore': 2,
                                        'common-semaphore': 1,
                                        'project1-semaphore': 0,
                                        'project2-semaphore': 1,
                                        'project3-semaphore': 0})
        self.assertSemaphores(tenant3, {'global-semaphore': 1,
                                        'common-semaphore': 1,
                                        'project1-semaphore': 0,
                                        'project2-semaphore': 0,
                                        'project3-semaphore': 1})

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()


class TestGlobalSemaphoresBroken(ZuulTestCase):
    validate_tenants = []
    tenant_config_file = 'config/global-semaphores-config/broken.yaml'
    # This test raises a config error during the startup of the test
    # case which makes the first scheduler fail during its startup.
    # The second (or any additional) scheduler won't even run as the
    # startup is serialized in tests/base.py.
    # Thus it doesn't make sense to execute this test with multiple
    # schedulers.
    scheduler_count = 1

    def setUp(self):
        self.assertRaises(zuul.configloader.GlobalSemaphoreNotFoundError,
                          super().setUp)

    def test_broken_global_semaphore_config(self):
        pass


class TestGlobalSemaphores(ZuulTestCase):
    tenant_config_file = 'config/global-semaphores/main.yaml'

    def test_global_semaphores(self):
        # This tests that a job finishing in one tenant will correctly
        # start a job in another tenant waiting on the semahpore.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([])
        self.assertBuilds([
            dict(name='test-global-semaphore', changes='1,1'),
        ])

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='test-global-semaphore',
                 result='SUCCESS', changes='1,1'),
            dict(name='test-global-semaphore',
                 result='SUCCESS', changes='2,1'),
        ], ordered=False)

        self.assertReportedStat(
            'zuul.global_semaphore.global-semaphore.holders',
            value='0', kind='g')
        self.assertReportedStat(
            'zuul.global_semaphore.global-semaphore.holders',
            value='1', kind='g')
        self.assertUnReportedStat(
            'zuul.tenant.tenant-one.semaphore.global-semaphore.holders')
