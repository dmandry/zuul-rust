# Copyright 2019 BMW Group
# Copyright 2023-2024 Acme Gating, LLC
#
# This module is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.

from collections import Counter
import fixtures
import re
import textwrap
import threading
import json

from zuul.model import PromoteEvent
import zuul.scheduler

from tests.base import (
    iterate_timeout,
    simple_layout,
    gerrit_config,
    ZuulGithubAppTestCase,
    ZuulTestCase,
)


class TestGerritCircularDependencies(ZuulTestCase):
    config_file = "zuul-gerrit-github.conf"
    tenant_config_file = "config/circular-dependencies/main.yaml"

    def _test_simple_cycle(self, project1, project2):
        A = self.fake_gerrit.addFakeChange(project1, "master", "A")
        B = self.fake_gerrit.addFakeChange(project2, "master", "B")

        # A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "1")

        # We're about to add approvals to changes without adding the
        # triggering events to Zuul, so that we can be sure that it is
        # enqueing the changes based on dependencies, not because of
        # triggering events.  Since it will have the changes cached
        # already (without approvals), we need to clear the cache
        # first.
        for connection in self.scheds.first.connections.connections.values():
            connection.maintainCache([], max_age=0)

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        A.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(B.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 3)
        self.assertEqual(B.reported, 3)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")

    def _test_transitive_cycle(self, project1, project2, project3):
        A = self.fake_gerrit.addFakeChange(project1, "master", "A")
        B = self.fake_gerrit.addFakeChange(project2, "master", "B")
        C = self.fake_gerrit.addFakeChange(project3, "master", "C")

        # A -> B -> C -> A (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, C.data["url"]
        )
        C.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            C.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(C.patchsets[-1]["approvals"]), 1)
        self.assertEqual(C.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(C.patchsets[-1]["approvals"][0]["value"], "1")

        # We're about to add approvals to changes without adding the
        # triggering events to Zuul, so that we can be sure that it is
        # enqueing the changes based on dependencies, not because of
        # triggering events.  Since it will have the changes cached
        # already (without approvals), we need to clear the cache
        # first.
        for connection in self.scheds.first.connections.connections.values():
            connection.maintainCache([], max_age=0)

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        C.addApproval("Code-Review", 2)
        C.addApproval("Approved", 1)
        A.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(B.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 3)
        self.assertEqual(B.reported, 3)
        self.assertEqual(C.reported, 3)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")
        self.assertEqual(C.data["status"], "MERGED")

    def test_single_project_cycle(self):
        self._test_simple_cycle("org/project", "org/project")

    def test_crd_cycle(self):
        self._test_simple_cycle("org/project1", "org/project2")

    def test_single_project_transitive_cycle(self):
        self._test_transitive_cycle(
            "org/project1", "org/project1", "org/project1"
        )

    def test_crd_transitive_cycle(self):
        self._test_transitive_cycle(
            "org/project", "org/project1", "org/project2"
        )

    def test_enqueue_order(self):
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B")
        C = self.fake_gerrit.addFakeChange("org/project2", "master", "C")

        # A <-> B and A -> C (via commit-depends)
        A.data[
            "commitMessage"
        ] = "{}\n\nDepends-On: {}\nDepends-On: {}\n".format(
            A.subject, B.data["url"], C.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(C.patchsets[-1]["approvals"]), 1)
        self.assertEqual(C.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(C.patchsets[-1]["approvals"][0]["value"], "1")

        # We're about to add approvals to changes without adding the
        # triggering events to Zuul, so that we can be sure that it is
        # enqueuing the changes based on dependencies, not because of
        # triggering events.  Since it will have the changes cached
        # already (without approvals), we need to clear the cache
        # first.
        for connection in self.scheds.first.connections.connections.values():
            connection.maintainCache([], max_age=0)

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        C.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)
        C.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 3)
        self.assertEqual(B.reported, 3)
        self.assertEqual(C.reported, 3)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")
        self.assertEqual(C.data["status"], "MERGED")

        self.assertHistory([
            # Change A (check + gate)
            dict(name="project1-job", result="SUCCESS", changes="3,1 2,1 1,1"),
            dict(name="project-vars-job", result="SUCCESS",
                 changes="3,1 2,1 1,1"),
            dict(name="project1-job", result="SUCCESS", changes="3,1 2,1 1,1"),
            dict(name="project-vars-job", result="SUCCESS",
                 changes="3,1 2,1 1,1"),
            # Change B (check + gate)
            dict(name="project-job", result="SUCCESS", changes="3,1 2,1 1,1"),
            dict(name="project-job", result="SUCCESS", changes="3,1 2,1 1,1"),
            # Change C (check + gate)
            dict(name="project2-job", result="SUCCESS", changes="3,1"),
            dict(name="project2-job", result="SUCCESS", changes="3,1"),
        ], ordered=False)

    def test_forbidden_cycle(self):
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project3", "master", "B")

        # A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "-1")

        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "-1")

        # We're about to add approvals to changes without adding the
        # triggering events to Zuul, so that we can be sure that it is
        # enqueing the changes based on dependencies, not because of
        # triggering events.  Since it will have the changes cached
        # already (without approvals), we need to clear the cache
        # first.
        for connection in self.scheds.first.connections.connections.values():
            connection.maintainCache([], max_age=0)

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 2)
        self.assertEqual(B.reported, 1)
        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(B.data["status"], "NEW")

    def test_git_dependency_with_cycle(self):
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project", "master", "B")
        C = self.fake_gerrit.addFakeChange("org/project1", "master", "C")

        # A -> B (git) -> C -> A
        A.setDependsOn(B, 1)
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, C.data["url"]
        )
        C.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            C.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(C.patchsets[-1]["approvals"]), 1)
        self.assertEqual(C.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(C.patchsets[-1]["approvals"][0]["value"], "1")

        # We're about to add approvals to changes without adding the
        # triggering events to Zuul, so that we can be sure that it is
        # enqueing the changes based on dependencies, not because of
        # triggering events.  Since it will have the changes cached
        # already (without approvals), we need to clear the cache
        # first.
        for connection in self.scheds.first.connections.connections.values():
            connection.maintainCache([], max_age=0)

        self.executor_server.hold_jobs_in_build = True
        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        C.addApproval("Code-Review", 2)
        A.addApproval("Approved", 1)
        B.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(C.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.reported, 3)
        self.assertEqual(B.reported, 3)
        self.assertEqual(C.reported, 3)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")
        self.assertEqual(C.data["status"], "MERGED")

    def test_dependency_on_cycle(self):
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B")
        C = self.fake_gerrit.addFakeChange("org/project2", "master", "C")

        # A -> B -> C -> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, C.data["url"]
        )
        C.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            C.subject, B.data["url"]
        )

        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(C.patchsets[-1]["approvals"]), 1)
        self.assertEqual(C.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(C.patchsets[-1]["approvals"][0]["value"], "1")

        # We're about to add approvals to changes without adding the
        # triggering events to Zuul, so that we can be sure that it is
        # enqueing the changes based on dependencies, not because of
        # triggering events.  Since it will have the changes cached
        # already (without approvals), we need to clear the cache
        # first.
        for connection in self.scheds.first.connections.connections.values():
            connection.maintainCache([], max_age=0)

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        C.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)
        C.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 3)
        self.assertEqual(B.reported, 3)
        self.assertEqual(C.reported, 3)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")
        self.assertEqual(C.data["status"], "MERGED")

    def test_dependency_on_merged_cycle(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B")
        C = self.fake_gerrit.addFakeChange("org/project2", "master", "C")

        # A -> B -> C -> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, C.data["url"]
        )
        C.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            C.subject, B.data["url"]
        )

        # Start jobs for A while B + C are still open so they get
        # enqueued as a non-live item ahead of A.
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Fake merge of change B + C while those changes are
        # still part of a non-live item as dependency for A.
        B.setMerged()
        C.setMerged()
        self.fake_gerrit.addEvent(B.getChangeMergedEvent())
        self.fake_gerrit.addEvent(C.getChangeMergedEvent())
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name="project-job", result="SUCCESS", changes="3,1 2,1 1,1"),
        ], ordered=False)

    def test_dependent_change_on_cycle(self):
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project", "master", "B")
        C = self.fake_gerrit.addFakeChange("org/project2", "master", "C")

        A.setDependsOn(B, 1)
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, C.data["url"]
        )
        C.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            C.subject, B.data["url"]
        )

        A.addApproval("Code-Review", 2)
        A.addApproval("Approved", 1)
        B.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)
        C.addApproval("Code-Review", 2)
        self.fake_gerrit.addEvent(C.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 3)

        # Make sure the out-of-cycle change (A) is enqueued after the cycle.
        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        queue_change_numbers = []
        for queue in tenant.layout.pipeline_managers["gate"].state.queues:
            for item in queue.queue:
                for change in item.changes:
                    queue_change_numbers.append(change.number)
        self.assertEqual(queue_change_numbers, ['2', '3', '1'])

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.reported, 2)
        self.assertEqual(B.reported, 2)
        self.assertEqual(C.reported, 2)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")
        self.assertEqual(C.data["status"], "MERGED")

    def test_cycle_dependency_on_cycle(self):
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B")
        C = self.fake_gerrit.addFakeChange("org/project1", "master", "C")
        D = self.fake_gerrit.addFakeChange("org/project2", "master", "D")

        # A -> B -> A + C
        # C -> D -> C
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data[
            "commitMessage"
        ] = "{}\n\nDepends-On: {}\nDepends-On: {}\n".format(
            B.subject, A.data["url"], C.data["url"]
        )
        C.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            C.subject, D.data["url"]
        )
        D.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            D.subject, C.data["url"]
        )

        self.fake_gerrit.addEvent(D.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(C.patchsets[-1]["approvals"]), 1)
        self.assertEqual(C.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(C.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(D.patchsets[-1]["approvals"]), 1)
        self.assertEqual(D.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(D.patchsets[-1]["approvals"][0]["value"], "1")

        # We're about to add approvals to changes without adding the
        # triggering events to Zuul, so that we can be sure that it is
        # enqueing the changes based on dependencies, not because of
        # triggering events.  Since it will have the changes cached
        # already (without approvals), we need to clear the cache
        # first.
        for connection in self.scheds.first.connections.connections.values():
            connection.maintainCache([], max_age=0)

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        C.addApproval("Code-Review", 2)
        D.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)
        C.addApproval("Approved", 1)
        D.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 3)
        self.assertEqual(B.reported, 3)
        self.assertEqual(C.reported, 3)
        self.assertEqual(D.reported, 3)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")
        self.assertEqual(C.data["status"], "MERGED")
        self.assertEqual(D.data["status"], "MERGED")

    def test_cycle_failure(self):
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B")

        # A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.executor_server.failJob("project-job", A)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "-1")

        # We're about to add approvals to changes without adding the
        # triggering events to Zuul, so that we can be sure that it is
        # enqueing the changes based on dependencies, not because of
        # triggering events.  Since it will have the changes cached
        # already (without approvals), we need to clear the cache
        # first.
        for connection in self.scheds.first.connections.connections.values():
            connection.maintainCache([], max_age=0)

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)

        self.executor_server.failJob("project-job", A)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 3)
        self.assertEqual(B.reported, 3)
        self.assertIn("cycle that failed", A.messages[-1])
        self.assertIn("cycle that failed", B.messages[-1])
        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(B.data["status"], "NEW")

    @simple_layout('layouts/circular-deps-node-failure.yaml')
    def test_cycle_failed_node_request(self):
        # Test a node request failure as part of a dependency cycle

        # Pause nodepool so we can fail the node request later
        self.fake_nodepool.pause()

        A = self.fake_gerrit.addFakeChange("org/project1", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project2", "master", "B")

        # A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)

        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        # Fail the node request and unpause
        req = self.fake_nodepool.getNodeRequests()
        self.fake_nodepool.addFailRequest(req[0])

        self.fake_nodepool.unpause()
        self.waitUntilSettled()

        self.assertEqual(A.reported, 2)
        self.assertEqual(B.reported, 2)
        self.assertIn("cycle that failed", A.messages[-1])
        self.assertIn("cycle that failed", B.messages[-1])
        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(B.data["status"], "NEW")

    def test_failing_cycle_behind_failing_change(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project", "master", "B")
        C = self.fake_gerrit.addFakeChange("org/project", "master", "C")
        D = self.fake_gerrit.addFakeChange("org/project", "master", "D")
        E = self.fake_gerrit.addFakeChange("org/project", "master", "E")

        # C <-> D (via commit-depends)
        C.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            C.subject, D.data["url"]
        )
        D.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            D.subject, C.data["url"]
        )

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        C.addApproval("Code-Review", 2)
        D.addApproval("Code-Review", 2)
        E.addApproval("Code-Review", 2)

        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.fake_gerrit.addEvent(B.addApproval("Approved", 1))
        # Make sure we enqueue C as part of the circular dependency with D, so
        # we end up with the following queue state: A, B, C, ...
        C.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(D.addApproval("Approved", 1))
        self.fake_gerrit.addEvent(E.addApproval("Approved", 1))
        self.waitUntilSettled()

        # Fail a job of the circular dependency
        self.executor_server.failJob("project-job", D)
        self.executor_server.release("project-job", change="4 1")

        # Fail job for item B ahead of the circular dependency so that this
        # causes a gate reset and item C and D are moved behind item A.
        self.executor_server.failJob("project-job", B)
        self.executor_server.release("project-job", change="2 1")
        self.waitUntilSettled()

        # Don't fail any other jobs
        self.executor_server.fail_tests.clear()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "NEW")
        self.assertEqual(C.data["status"], "MERGED")
        self.assertEqual(D.data["status"], "MERGED")
        self.assertEqual(E.data["status"], "MERGED")

        self.assertHistory([
            dict(name="project-job", result="SUCCESS", changes="1,1"),
            dict(name="project-job", result="FAILURE", changes="1,1 2,1"),
            # First attempt of change C and D before gate reset due to change B
            dict(name="project-job", result="FAILURE",
                 changes="1,1 2,1 3,1 4,1"),
            dict(name="project-job", result="FAILURE",
                 changes="1,1 2,1 3,1 4,1"),
            dict(name="project-job", result="ABORTED",
                 changes="1,1 2,1 3,1 4,1 5,1"),
            dict(name="project-job", result="SUCCESS", changes="1,1 3,1 4,1"),
            dict(name="project-job", result="SUCCESS", changes="1,1 3,1 4,1"),
            dict(name="project-job", result="SUCCESS",
                 changes="1,1 3,1 4,1 5,1"),
        ], ordered=False)

    def test_dependency_on_cycle_failure(self):
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B")
        C = self.fake_gerrit.addFakeChange("org/project2", "master", "C")
        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)
        C.addApproval("Code-Review", 2)
        C.addApproval("Approved", 1)

        # A -> B -> C -> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, C.data["url"]
        )
        C.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            C.subject, B.data["url"]
        )

        self.executor_server.failJob("project2-job", C)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertIn("depends on a change that failed to merge",
                      A.messages[-1])
        self.assertTrue(re.search(r'Change http://localhost:\d+/'
                                  r'c/org/project1/\+/2 is needed',
                        A.messages[-1]))
        self.assertFalse(re.search('Change .*? can not be merged',
                         A.messages[-1]))

        self.assertIn("cycle that failed.", B.messages[-1])
        self.assertFalse(re.search('Change http://localhost:.*? is needed',
                         B.messages[-1]))
        self.assertFalse(re.search('Change .*? can not be merged',
                         B.messages[-1]))

        self.assertIn("cycle that failed.", C.messages[-1])
        self.assertFalse(re.search('Change http://localhost:.*? is needed',
                         C.messages[-1]))
        self.assertFalse(re.search('Change .*? can not be merged',
                         C.messages[-1]))

        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(B.data["status"], "NEW")
        self.assertEqual(C.data["status"], "NEW")

    def test_cycle_dependency_on_change(self):
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B")
        C = self.fake_gerrit.addFakeChange("org/project2", "master", "C")

        # A -> B -> A + C (git)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )
        B.setDependsOn(C, 1)

        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(C.patchsets[-1]["approvals"]), 1)
        self.assertEqual(C.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(C.patchsets[-1]["approvals"][0]["value"], "1")

        # We're about to add approvals to changes without adding the
        # triggering events to Zuul, so that we can be sure that it is
        # enqueing the changes based on dependencies, not because of
        # triggering events.  Since it will have the changes cached
        # already (without approvals), we need to clear the cache
        # first.
        for connection in self.scheds.first.connections.connections.values():
            connection.maintainCache([], max_age=0)

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        C.addApproval("Code-Review", 2)
        A.addApproval("Approved", 1)
        B.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(C.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 3)
        self.assertEqual(B.reported, 3)
        self.assertEqual(C.reported, 3)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")
        self.assertEqual(C.data["status"], "MERGED")

    def test_failing_cycle_dependency_on_change(self):
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B")
        C = self.fake_gerrit.addFakeChange("org/project2", "master", "C")
        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)
        C.addApproval("Code-Review", 2)
        C.addApproval("Approved", 1)

        # A -> B -> A + C (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data[
            "commitMessage"
        ] = "{}\n\nDepends-On: {}\nDepends-On: {}\n".format(
            B.subject, A.data["url"], C.data["url"]
        )

        self.executor_server.failJob("project-job", A)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 2)
        self.assertEqual(B.reported, 2)
        self.assertEqual(C.reported, 2)
        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(B.data["status"], "NEW")
        self.assertEqual(C.data["status"], "MERGED")

    def test_reopen_cycle(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project2", "master", "B")

        # A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        A.addApproval("Approved", 1)

        self.fake_gerrit.addEvent(B.addApproval("Approved", 1))
        self.waitUntilSettled()

        items_before = self.getAllItems('tenant-one', 'gate')

        # Trigger a re-enqueue of change B
        self.fake_gerrit.addEvent(B.getChangeAbandonedEvent())
        self.fake_gerrit.addEvent(B.getChangeRestoredEvent())
        self.fake_gerrit.addEvent(B.addApproval("Approved", 1))
        self.waitUntilSettled()

        items_after = self.getAllItems('tenant-one', 'gate')

        # Make sure the complete cycle was re-enqueued
        for before, after in zip(items_before, items_after):
            self.assertNotEqual(before, after)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        # Start, Dequeue (cycle abandoned), Start, Success
        self.assertEqual(A.reported, 4)
        self.assertEqual(B.reported, 4)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")

    def test_cycle_larger_pipeline_window(self):
        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")

        # Make the gate window smaller than the length of the cycle
        for queue in tenant.layout.pipeline_managers["gate"].state.queues:
            if any("org/project" in p.name for p in queue.projects):
                queue.window = 1

        self._test_simple_cycle("org/project", "org/project")

    def test_cycle_reporting_failure(self):
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B")
        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)

        B.fail_merge = True

        # A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 3)
        self.assertEqual(B.reported, 3)
        self.assertEqual(A.patchsets[-1]["approvals"][-1]["value"], "-2")
        self.assertEqual(B.patchsets[-1]["approvals"][-1]["value"], "-2")
        self.assertIn("cycle that failed", A.messages[-1])
        self.assertIn("cycle that failed", B.messages[-1])
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "NEW")

        buildsets = self.scheds.first.connections.connections[
            'database'].getBuildsets()
        self.assertEqual(len(buildsets), 1)
        self.assertEqual(buildsets[0].result, 'MERGE_FAILURE')

    def test_cycle_reporting_partial_failure(self):
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B")
        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)

        A.fail_merge = True

        # A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 3)
        self.assertEqual(B.reported, 3)
        self.assertIn("cycle that failed", A.messages[-1])
        self.assertIn("cycle that failed", B.messages[-1])
        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(B.data["status"], "MERGED")

    def test_gate_reset_with_cycle(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B")
        C = self.fake_gerrit.addFakeChange("org/project1", "master", "C")

        # A <-> B (via depends-on)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        C.addApproval("Code-Review", 2)
        C.addApproval("Approved", 1)
        B.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(C.addApproval("Approved", 1))
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.executor_server.failJob("project1-job", C)
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 3)
        for build in self.builds:
            self.assertTrue(build.hasChanges(A, B))
            self.assertFalse(build.hasChanges(C))

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.reported, 2)
        self.assertEqual(B.reported, 2)
        self.assertEqual(C.reported, 2)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")
        self.assertEqual(C.data["status"], "NEW")

    def test_independent_cycle_items(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project", "master", "B")

        # A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        manager = tenant.layout.pipeline_managers["check"]
        self.assertEqual(len(manager.state.queues), 1)
        queue = manager.state.queues[0].queue
        self.assertEqual(len(queue), 1)

        self.assertEqual(len(self.builds), 2)
        for build in self.builds:
            self.assertTrue(build.hasChanges(A, B))

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "1")

    def test_gate_correct_commits(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B")
        C = self.fake_gerrit.addFakeChange("org/project1", "master", "C")
        D = self.fake_gerrit.addFakeChange("org/project", "master", "D")

        # A <-> B (via depends-on)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )
        D.setDependsOn(A, 1)

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        C.addApproval("Code-Review", 2)
        D.addApproval("Code-Review", 2)
        C.addApproval("Approved", 1)
        B.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(C.addApproval("Approved", 1))
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.fake_gerrit.addEvent(D.addApproval("Approved", 1))
        self.waitUntilSettled()

        for build in self.builds:
            if build.change in ("1 1", "2 1"):
                self.assertTrue(build.hasChanges(C, B, A))
                self.assertFalse(build.hasChanges(D))
            elif build.change == "3 1":
                self.assertTrue(build.hasChanges(C))
                self.assertFalse(build.hasChanges(A))
                self.assertFalse(build.hasChanges(B))
                self.assertFalse(build.hasChanges(D))
            else:
                self.assertTrue(build.hasChanges(C, B, A, D))

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.reported, 2)
        self.assertEqual(B.reported, 2)
        self.assertEqual(C.reported, 2)
        self.assertEqual(D.reported, 2)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")
        self.assertEqual(C.data["status"], "MERGED")
        self.assertEqual(D.data["status"], "MERGED")

    def test_cycle_git_dependency(self):
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project", "master", "B")
        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)

        # A -> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        # B -> A (via parent-child dependency)
        B.setDependsOn(A, 1)

        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 2)
        self.assertEqual(B.reported, 2)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")

    def test_cycle_git_dependency2(self):
        # Reverse the enqueue order to make sure both cases are
        # tested.
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project", "master", "B")
        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        A.addApproval("Approved", 1)

        # A -> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        # B -> A (via parent-child dependency)
        B.setDependsOn(A, 1)

        self.fake_gerrit.addEvent(B.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 2)
        self.assertEqual(B.reported, 2)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")

    def test_cycle_git_dependency_failure(self):
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project", "master", "B")
        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)

        # A -> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        # B -> A (via parent-child dependency)
        B.setDependsOn(A, 1)

        self.executor_server.failJob("project-job", A)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 2)
        self.assertEqual(B.reported, 2)
        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(B.data["status"], "NEW")

    def test_independent_reporting(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project", "master", "B")

        # A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.fake_gerrit.addEvent(B.getChangeAbandonedEvent())
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "-1")
        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "-1")

    def test_abandon_cancel_jobs(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange("org/project7", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project7", "master", "B")

        # A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.fake_gerrit.addEvent(B.getChangeAbandonedEvent())
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name="project7-parent-job", result="ABORTED",
                 changes="2,1 1,1"),
        ], ordered=False)

    def test_cycle_merge_conflict(self):
        M = self.fake_gerrit.addFakeChange('org/project', 'master', 'M',
                                           files={'conflict': 'foo'})
        M.setMerged()
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files={'conflict': 'bar'})
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')

        # A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)

        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 1)
        self.assertEqual(B.reported, 1)
        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(B.data["status"], "NEW")

    def test_circular_config_change(self):
        define_job = textwrap.dedent(
            """
            - job:
                name: new-job
            """)
        use_job = textwrap.dedent(
            """
            - project:
                queue: integrated
                check:
                  jobs:
                    - new-job
                gate:
                  jobs:
                    - new-job
            """)
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A",
                                           files={"zuul.yaml": define_job})
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B",
                                           files={"zuul.yaml": use_job})

        # A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")
        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "1")

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        self.fake_gerrit.addEvent(B.addApproval("Approved", 1))
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 3)
        self.assertEqual(B.reported, 3)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")

    def test_circular_config_change_job_vars(self):
        org_project_files = {
            "zuul.yaml": textwrap.dedent(
                """
                - job:
                    name: project-vars-job
                    deduplicate: false
                    vars:
                      test_var: pass

                - project:
                    queue: integrated
                    check:
                      jobs:
                        - project-vars-job
                    gate:
                      jobs:
                        - project-vars-job
                """)
        }
        A = self.fake_gerrit.addFakeChange("org/project2", "master", "A",
                                           files=org_project_files)
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B")
        C = self.fake_gerrit.addFakeChange("org/project1", "master", "C")

        # C <-> A <-> B (via commit-depends)
        A.data["commitMessage"] = (
            "{}\n\nDepends-On: {}\nDepends-On: {}\n".format(
                A.subject, B.data["url"], C.data["url"]
            )
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )
        C.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            C.subject, A.data["url"]
        )

        self.executor_server.hold_jobs_in_build = True
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        vars_builds = [b for b in self.builds if b.name == "project-vars-job"]
        self.assertEqual(len(vars_builds), 3)
        self.assertEqual(vars_builds[0].job.variables["test_var"], "pass")

        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(C.patchsets[-1]["approvals"]), 1)
        self.assertEqual(C.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(C.patchsets[-1]["approvals"][0]["value"], "1")

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        C.addApproval("Code-Review", 2)

        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.fake_gerrit.addEvent(B.addApproval("Approved", 1))
        self.fake_gerrit.addEvent(C.addApproval("Approved", 1))
        self.waitUntilSettled()

        vars_builds = [b for b in self.builds if b.name == "project-vars-job"]
        self.assertEqual(len(vars_builds), 3)
        for build in vars_builds:
            self.assertEqual(build.job.variables["test_var"], "pass")

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.reported, 3)
        self.assertEqual(B.reported, 3)
        self.assertEqual(C.reported, 3)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")
        self.assertEqual(C.data["status"], "MERGED")

    def test_circular_config_change_single_merge_job(self):
        """Regression tests to make sure that a cycle with non-live
        config changes only spawns one merge job (so that we avoid
        problems with multiple jobs arriving in the wrong order)."""

        define_job = textwrap.dedent(
            """
            - job:
                name: new-job
            """)
        use_job = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - new-job
                gate:
                  jobs:
                    - new-job
            """)
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A",
                                           files={"zuul.yaml": define_job})
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B",
                                           files={"zuul.yaml": use_job})

        # A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.waitUntilSettled()
        self.hold_merge_jobs_in_queue = True

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Assert that there is a single merge job for the cycle.
        self.assertEqual(len(self.merger_api.queued()), 1)

        self.hold_merge_jobs_in_queue = False
        self.merger_api.release()
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")

    def test_bundle_id_in_zuul_var(self):
        A = self.fake_gerrit.addFakeChange("org/project1", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B")
        C = self.fake_gerrit.addFakeChange("org/project1", "master", "C")

        # A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.executor_server.hold_jobs_in_build = True

        # bundle_id should be in check build of A,B
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        var_zuul_items = self.builds[0].parameters["zuul"]["items"]
        self.assertEqual(len(var_zuul_items), 2)
        self.assertIn("bundle_id", var_zuul_items[0])
        bundle_id_0 = var_zuul_items[0]["bundle_id"]
        self.assertIn("bundle_id", var_zuul_items[1])
        bundle_id_1 = var_zuul_items[1]["bundle_id"]
        self.assertEqual(bundle_id_0, bundle_id_1)
        self.executor_server.release()
        self.waitUntilSettled()
        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")
        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "1")

        # bundle_id should not be in check build of C
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        var_zuul_items = self.builds[0].parameters["zuul"]["items"]
        self.assertEqual(len(var_zuul_items), 1)
        self.assertNotIn("bundle_id", var_zuul_items[0])
        self.executor_server.release()
        self.waitUntilSettled()
        self.assertEqual(len(C.patchsets[-1]["approvals"]), 1)
        self.assertEqual(C.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(C.patchsets[-1]["approvals"][0]["value"], "1")

        # bundle_id should be in gate jobs of A and B, but not in C
        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        C.addApproval("Code-Review", 2)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.fake_gerrit.addEvent(B.addApproval("Approved", 1))
        self.fake_gerrit.addEvent(C.addApproval("Approved", 1))
        self.waitUntilSettled()
        var_zuul_items = self.builds[-1].parameters["zuul"]["items"]
        self.assertEqual(len(var_zuul_items), 3)
        self.assertIn("bundle_id", var_zuul_items[0])
        bundle_id_0 = var_zuul_items[0]["bundle_id"]
        self.assertIn("bundle_id", var_zuul_items[1])
        bundle_id_1 = var_zuul_items[1]["bundle_id"]
        self.assertEqual(bundle_id_0, bundle_id_1)
        self.assertNotIn("bundle_id", var_zuul_items[2])

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.reported, 3)
        self.assertEqual(B.reported, 3)
        self.assertEqual(C.reported, 3)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")
        self.assertEqual(C.data["status"], "MERGED")

    def test_cross_tenant_cycle(self):
        org_project_files = {
            "zuul.yaml": textwrap.dedent(
                """
                - job:
                    name: project-vars-job
                    vars:
                      test_var: pass

                - project:
                    queue: integrated
                    check:
                      jobs:
                        - project-vars-job
                    gate:
                      jobs:
                        - project-vars-job
                """)
        }
        # Change zuul config so the cycle is considered updating config
        A = self.fake_gerrit.addFakeChange("org/project2", "master", "A",
                                           files=org_project_files)
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B")
        C = self.fake_gerrit.addFakeChange("org/project1", "master", "C")
        D = self.fake_gerrit.addFakeChange("org/project4", "master", "D",)

        # C <-> A <-> B (via commit-depends)
        A.data["commitMessage"] = (
            "{}\n\nDepends-On: {}\nDepends-On: {}\n".format(
                A.subject, B.data["url"], C.data["url"]
            )
        )
        # A <-> B (via commit-depends)
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )
        # A <-> C <-> D (via commit-depends)
        C.data["commitMessage"] = (
            "{}\n\nDepends-On: {}\nDepends-On: {}\n".format(
                C.subject, A.data["url"], D.data["url"]
            )
        )
        # D <-> C (via commit-depends)
        D.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            D.subject, C.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "-1")

        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "-1")

        self.assertEqual(len(C.patchsets[-1]["approvals"]), 1)
        self.assertEqual(C.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(C.patchsets[-1]["approvals"][0]["value"], "-1")

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        C.addApproval("Code-Review", 2)
        D.addApproval("Code-Review", 2)

        # Add the approvals first, then add the events to ensure we
        # are not racing gerrit approval changes.
        events = [c.addApproval("Approved", 1) for c in [A, B, C, D]]
        for event in events:
            self.fake_gerrit.addEvent(event)
        self.waitUntilSettled()

        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(B.data["status"], "NEW")
        self.assertEqual(C.data["status"], "NEW")

        D.setMerged()
        for connection in self.scheds.first.connections.connections.values():
            connection.maintainCache([], max_age=0)

        # Pretend D was merged so we can gate the cycle
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        # Two failures, and one success (start and end)
        self.assertEqual(A.reported, 4)
        self.assertEqual(B.reported, 4)
        self.assertEqual(C.reported, 4)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")
        self.assertEqual(C.data["status"], "MERGED")

    def test_cycle_unknown_repo(self):
        self.init_repo("org/unknown", tag='init')
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/unknown", "master", "B")

        # A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "-1")

        for connection in self.scheds.first.connections.connections.values():
            connection.maintainCache([], max_age=0)

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 2)
        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(B.data["status"], "NEW")

        for connection in self.scheds.first.connections.connections.values():
            connection.maintainCache([], max_age=0)

        B.setMerged()
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 4)
        self.assertEqual(A.data["status"], "MERGED")

    def test_promote_cycle(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B")
        C = self.fake_gerrit.addFakeChange("org/project2", "master", "C")

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        C.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(C.addApproval("Approved", 1))
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        event = PromoteEvent('tenant-one', 'gate', ["2,1"])
        self.scheds.first.sched.pipeline_management_events['tenant-one'][
            'gate'].put(event)
        self.waitUntilSettled()

        self.assertEqual(len(self.builds), 4)
        self.assertTrue(self.builds[0].hasChanges(A))
        self.assertTrue(self.builds[0].hasChanges(B))
        self.assertFalse(self.builds[0].hasChanges(C))

        self.assertTrue(self.builds[1].hasChanges(A))
        self.assertTrue(self.builds[1].hasChanges(B))
        self.assertFalse(self.builds[0].hasChanges(C))

        self.assertTrue(self.builds[3].hasChanges(B))
        self.assertTrue(self.builds[3].hasChanges(C))
        self.assertTrue(self.builds[3].hasChanges(A))

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.reported, 2)
        self.assertEqual(B.reported, 2)
        self.assertEqual(C.reported, 2)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")
        self.assertEqual(C.data["status"], "MERGED")

    def test_shared_queue_removed(self):
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        C.addApproval('Code-Review', 2)

        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))

        self.waitUntilSettled()
        self.executor_server.release('.*-merge')
        self.waitUntilSettled()
        self.executor_server.release('.*-merge')
        self.waitUntilSettled()

        # Remove the shared queue.
        self.commitConfigUpdate(
            'common-config',
            'layouts/circular-dependency-shared-queue-removed.yaml')

        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(B.data['status'], 'NEW')
        self.assertEqual(C.data['status'], 'MERGED')

    def _test_job_deduplication(self):
        # We make a second scheduler here so that the first scheduler
        # can freeze the jobs for the first item, and the second
        # scheduler freeze jobs for the second.  This forces the
        # scheduler to compare a desiralized FrozenJob with a newly
        # created one and therefore show difference-in-serialization
        # issues.
        self.hold_merge_jobs_in_queue = True
        app = self.createScheduler()
        app.start()
        self.assertEqual(len(self.scheds), 2)

        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)

        with app.sched.run_handler_lock:
            self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
            self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
            self.waitUntilSettled(matcher=[self.scheds.first])
            self.merger_api.release(self.merger_api.queued()[0])
            self.waitUntilSettled(matcher=[self.scheds.first])

        # Hold the lock on the first scheduler so that only the second
        # will act.
        with self.scheds.first.sched.run_handler_lock:
            self.merger_api.release()
            self.waitUntilSettled(matcher=[app])

        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(B.data['status'], 'MERGED')

    @simple_layout('layouts/job-dedup-auto-shared.yaml')
    def test_job_deduplication_auto_shared(self):
        self._test_job_deduplication()
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 3)

    @simple_layout('layouts/job-dedup-auto-unshared.yaml')
    def test_job_deduplication_auto_unshared(self):
        self._test_job_deduplication()
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/02/2/1'),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is not deduplicated
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/01/1/1'),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 4)

    @simple_layout('layouts/job-dedup-true.yaml')
    def test_job_deduplication_true(self):
        self._test_job_deduplication()
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 3)

    @simple_layout('layouts/job-dedup-false.yaml')
    def test_job_deduplication_false(self):
        self._test_job_deduplication()
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/02/2/1'),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is not deduplicated, though it would be under auto
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/01/1/1'),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 4)

    @simple_layout('layouts/job-dedup-empty-nodeset.yaml')
    def test_job_deduplication_empty_nodeset(self):
        # Make sure that jobs with empty nodesets can still be
        # deduplicated
        self._test_job_deduplication()
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 0)

    @simple_layout('layouts/job-dedup-branches.yaml')
    def test_job_deduplication_job_branches(self):
        # Make sure that jobs with different branch matchers can still
        # be deduplicated
        self._test_job_deduplication()
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 0)

    @simple_layout('layouts/job-dedup-child-jobs.yaml')
    def test_job_deduplication_child_jobs(self):
        # Test that child jobs of deduplicated parents are
        # deduplicated, and also that supplying child_jobs to
        # zuul_return filters correctly.  child1-job should not run,
        # but child2 should run and be deduplicated.  This uses auto
        # deduplication.
        self.executor_server.returnData(
            'common-job', 'refs/changes/02/2/1',
            {'zuul': {'child_jobs': ['child2-job']}}
        )
        self.executor_server.returnData(
            'common-job', 'refs/changes/01/1/1',
            {'zuul': {'child_jobs': ['child2-job']}}
        )

        self._test_job_deduplication()
        self.assertHistory([
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="child2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            # dict(name="child2-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)

    @simple_layout('layouts/job-dedup-mismatched-child-jobs.yaml')
    def test_job_deduplication_mismatched_child_jobs(self):
        # Test that a parent job with different child jobs is
        # deduplicated.  This uses auto-deduplication.
        self._test_job_deduplication()
        self.assertHistory([
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="child1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="child2-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)

    @simple_layout('layouts/job-dedup-child-of-diff-parent.yaml')
    def test_job_deduplication_child_of_diff_parent_diff_data(self):
        # The common job is forced to not deduplicate, and the child
        # job is deduplicated.  The child job treats each of the
        # common jobs as a parent.
        self.executor_server.returnData(
            'common-job', 'refs/changes/02/2/1',
            {'foo': 'a'}
        )
        self.executor_server.returnData(
            'common-job', 'refs/changes/01/1/1',
            {'bar': 'b'}
        )
        self._test_job_deduplication()
        self.assertHistory([
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/02/2/1'),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/01/1/1'),
            dict(name="child-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/01/1/1'),
        ], ordered=False)
        # child-job for 2 is deduplicated into 1 (the triggering change).
        job = self.getJobFromHistory('child-job')
        self.assertEqual({'foo': 'a', 'bar': 'b'},
                         job.parameters['parent_data'])

    @simple_layout('layouts/job-dedup-paused-parent.yaml')
    def test_job_deduplication_paused_parent(self):
        # Pause a parent job
        # Ensure it waits for all children before continuing

        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )
        self.executor_server.returnData(
            'common-job', A,
            {'zuul': {'pause': True}}
        )
        self.executor_server.returnData(
            'common-job', B,
            {'zuul': {'pause': True}}
        )

        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)

        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        self.waitUntilSettled()

        self.executor_server.release('common-job')
        self.waitUntilSettled()
        self.executor_server.release('project1-job')
        self.waitUntilSettled()

        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)

        self.assertEqual(len(self.builds), 2)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1",),
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)

    @simple_layout('layouts/job-dedup-auto-shared.yaml')
    def test_job_deduplication_failed_node_request(self):
        # Pause nodepool so we can fail the node request later
        self.fake_nodepool.pause()

        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)

        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        self.waitUntilSettled()

        # Fail the node request and unpause
        items = self.getAllItems('tenant-one', 'gate')
        job = items[0].current_build_set.job_graph.getJob(
            'common-job', items[0].changes[0].cache_key)
        for req in self.fake_nodepool.getNodeRequests():
            if req['requestor_data']['job_uuid'] == job.uuid:
                self.fake_nodepool.addFailRequest(req)

        self.fake_nodepool.unpause()
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(B.data['status'], 'NEW')
        # This would previously fail both items in the bundle as soon
        # as the bundle as a whole started failing.  The new/current
        # behavior is more like non-bundle items, in that the item
        # will continue running jobs even after one job fails.
        # common-job does not appear in history due to the node failure
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 3)

    @simple_layout('layouts/job-dedup-auto-shared.yaml')
    def test_job_deduplication_failed_job(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        self.executor_server.failJob("common-job", A)

        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        # If we don't make sure these jobs finish first, then one of
        # the items may complete before the other and cause Zuul to
        # abort the project*-job on the other item (with a "bundle
        # failed to merge" error).
        self.waitUntilSettled()
        self.executor_server.release('project1-job')
        self.executor_server.release('project2-job')
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(B.data['status'], 'NEW')
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="FAILURE", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 3)

    @simple_layout('layouts/job-dedup-false.yaml')
    def test_job_deduplication_false_failed_job(self):
        # Test that if we are *not* deduplicating jobs, we don't
        # duplicate the result on two different builds.
        # The way we check that is to retry the common-job between two
        # items, but only once, and only on one item.  The other item
        # should be unaffected.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)

        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        # If we don't make sure these jobs finish first, then one of
        # the items may complete before the other and cause Zuul to
        # abort the project*-job on the other item (with a "bundle
        # failed to merge" error).
        self.waitUntilSettled()
        for build in self.builds:
            if build.name == 'common-job' and build.project == 'org/project1':
                break
        else:
            raise Exception("Unable to find build")
        build.should_retry = True

        self.executor_server.release('project1-job')
        self.executor_server.release('project2-job')
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(B.data['status'], 'MERGED')
        self.assertHistory([
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result=None, changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 5)

    @simple_layout('layouts/job-dedup-auto-shared.yaml')
    def test_job_deduplication_multi_scheduler(self):
        # Test that a second scheduler can correctly refresh
        # deduplicated builds
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)

        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        self.waitUntilSettled()

        app = self.createScheduler()
        app.start()
        self.assertEqual(len(self.scheds), 2)

        # Hold the lock on the first scheduler so that only the second
        # will act.
        with self.scheds.first.sched.run_handler_lock:
            self.executor_server.hold_jobs_in_build = False
            self.executor_server.release()
            self.waitUntilSettled(matcher=[app])

        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(B.data['status'], 'MERGED')
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)

    @simple_layout('layouts/job-dedup-multi-sched-complete.yaml')
    def test_job_deduplication_multi_scheduler_complete(self):
        # Test that a second scheduler can correctly refresh
        # deduplicated builds after the first scheduler has completed
        # the builds for one item.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)

        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        self.executor_server.release('common-job')
        self.executor_server.release('project1-job')
        self.waitUntilSettled()

        app = self.createScheduler()
        app.start()
        self.assertEqual(len(self.scheds), 2)

        # Hold the lock on the first scheduler so that only the second
        # will act.
        with self.scheds.first.sched.run_handler_lock:
            self.executor_server.hold_jobs_in_build = False

            # Make sure we go through an extra pipeline cycle so that
            # the cleanup method runs.
            self.executor_server.release('project2-job2')
            self.waitUntilSettled(matcher=[app])

            self.executor_server.release()
            self.waitUntilSettled(matcher=[app])

        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job2", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)

    @simple_layout('layouts/job-dedup-multi-sched-complete.yaml')
    def test_job_deduplication_multi_scheduler_complete2(self):
        # Test that a second scheduler can correctly refresh
        # deduplicated builds after the first scheduler has completed
        # the builds for one item.

        # This is similar to the previous test but starts the second
        # scheduler slightly later.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )
        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)

        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.executor_server.release('common-job')
        self.waitUntilSettled()

        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.executor_server.release('project1-job')
        self.waitUntilSettled()

        app = self.createScheduler()
        app.start()
        self.assertEqual(len(self.scheds), 2)

        # Hold the lock on the first scheduler so that only the second
        # will act.
        with self.scheds.first.sched.run_handler_lock:
            self.executor_server.hold_jobs_in_build = False

            self.executor_server.release('project2-job2')
            self.waitUntilSettled(matcher=[app])

            self.executor_server.release()
            self.waitUntilSettled(matcher=[app])

        # We expect common-job to be "un-deduplicated".
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="1,1 2,1"),
            dict(name="common-job", result="SUCCESS", changes="1,1 2,1"),
            dict(name="project2-job", result="SUCCESS", changes="1,1 2,1"),
            dict(name="project2-job2", result="SUCCESS", changes="1,1 2,1"),
        ], ordered=False)

    @simple_layout('layouts/job-dedup-noop.yaml')
    def test_job_deduplication_noop(self):
        # Test that we deduplicate noop (there's no good reason not
        # to do so)
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)

        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(B.data['status'], 'MERGED')
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        # It's tricky to get info about a noop build, but the jobs in
        # the report have the build UUID, so we make sure it's
        # the same and we only have one noop job.
        a_noop = [l for l in A.messages[-1].split('\n') if 'noop' in l]
        b_noop = [l for l in B.messages[-1].split('\n') if 'noop' in l]
        self.assertEqual(a_noop, b_noop)
        self.assertEqual(len(a_noop), 1)

    @simple_layout('layouts/job-dedup-retry.yaml')
    def test_job_deduplication_retry(self):
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.executor_server.retryJob('common-job', A)

        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)

        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(B.data['status'], 'NEW')
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # There should be exactly 3 runs of the job (not 6)
            dict(name="common-job", result=None, changes="2,1 1,1"),
            dict(name="common-job", result=None, changes="2,1 1,1"),
            dict(name="common-job", result=None, changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 5)

    @simple_layout('layouts/job-dedup-retry-child.yaml')
    def test_job_deduplication_retry_child(self):
        # This tests retrying a paused build (simulating an executor restart)
        # See test_data_return_child_from_retried_paused_job
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        # A is the triggering change for the deduplicated parent-job
        self.executor_server.returnData(
            'parent-job', A,
            {'zuul': {'pause': True}}
        )

        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)

        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        self.waitUntilSettled()

        self.executor_server.release('parent-job')
        self.waitUntilSettled("till job is paused")

        paused_job = self.builds[0]
        self.assertTrue(paused_job.paused)

        # Stop the job worker to simulate an executor restart
        for job_worker in self.executor_server.job_workers.values():
            if job_worker.build_request.uuid == paused_job.uuid:
                job_worker.stop()
        self.waitUntilSettled("stop job worker")

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled("all jobs are done")
        # The "pause" job might be paused during the waitUntilSettled
        # call and appear settled; it should automatically resume
        # though, so just wait for it.
        for x in iterate_timeout(60, 'paused job'):
            if not self.builds:
                break
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(B.data['status'], 'MERGED')
        self.assertHistory([
            dict(name="parent-job", result="ABORTED", changes="2,1 1,1"),
            dict(name="project1-job", result="ABORTED", changes="2,1 1,1"),
            dict(name="project2-job", result="ABORTED", changes="2,1 1,1"),
            dict(name="parent-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 6)

    @simple_layout('layouts/job-dedup-parent-data.yaml')
    def test_job_deduplication_parent_data(self):
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        # The parent job returns data
        self.executor_server.returnData(
            'parent-job', A,
            {'zuul':
             {'artifacts': [
                 {'name': 'image',
                  'url': 'http://example.com/image',
                  'metadata': {
                      'type': 'container_image'
                  }},
             ]}}
        )

        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(B.data['status'], 'MERGED')
        self.assertHistory([
            dict(name="parent-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # Only one run of the common job since it's the same
            dict(name="common-child-job", result="SUCCESS", changes="2,1 1,1"),
            # The forked job depends on different parents
            # so it should run twice
            dict(name="forked-child-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="forked-child-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 6)

    def _test_job_deduplication_semaphore(self):
        "Test semaphores with max=1 (mutex) and get resources first"
        self.executor_server.hold_jobs_in_build = True

        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(
            len(tenant.semaphore_handler.semaphoreHolders("test-semaphore")),
            0)

        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        self.waitUntilSettled()
        self.assertEqual(
            len(tenant.semaphore_handler.semaphoreHolders("test-semaphore")),
            1)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 3)
        self.assertEqual(
            len(tenant.semaphore_handler.semaphoreHolders("test-semaphore")),
            0)

    @simple_layout('layouts/job-dedup-semaphore.yaml')
    def test_job_deduplication_semaphore(self):
        self._test_job_deduplication_semaphore()

    @simple_layout('layouts/job-dedup-semaphore-first.yaml')
    def test_job_deduplication_semaphore_resources_first(self):
        self._test_job_deduplication_semaphore()

    def _merge_noop_pair(self):
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)

        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))

        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(B.data['status'], 'MERGED')

    @simple_layout('layouts/job-dedup-noop.yaml')
    def test_job_deduplication_buildsets(self):
        self._merge_noop_pair()
        self._merge_noop_pair()
        self._merge_noop_pair()

        buildsets = self.scheds.first.connections.connections[
            'database'].getBuildsets()
        self.assertEqual(len(buildsets), 3)

        # If we accidentally limit by rows, we should get fewer than 2
        # buildsets returned here.
        buildsets = self.scheds.first.connections.connections[
            'database'].getBuildsets(limit=2)
        self.assertEqual(len(buildsets), 2)

    # Start check tests

    def _test_job_deduplication_check(self, files_a=None, files_b=None):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=files_a)
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B',
                                           files=files_b)

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.executor_server.release('common-job')
        self.executor_server.release('project1-job')
        self.waitUntilSettled()

        # We do this even though it results in no changes to force an
        # extra pipeline processing run to make sure we don't garbage
        # collect the item early.
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.executor_server.release('project2-job')
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

    def _assert_job_deduplication_check(self):
        # Make sure there are no leaked queue items
        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        manager = tenant.layout.pipeline_managers["check"]
        pipeline_path = manager.state.getPath()
        all_items = set(self.zk_client.client.get_children(
            f"{pipeline_path}/item"))
        self.assertEqual(len(all_items), 0)
        self.assertEqual(len(self.fake_nodepool.history), len(self.history))

    @simple_layout('layouts/job-dedup-auto-shared.yaml')
    def test_job_deduplication_check_auto_shared(self):
        self._test_job_deduplication_check()
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self._assert_job_deduplication_check()

    @simple_layout('layouts/job-dedup-auto-unshared.yaml')
    def test_job_deduplication_check_auto_unshared(self):
        self._test_job_deduplication_check()
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/01/1/1'),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is not deduplicated
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/02/2/1'),
        ], ordered=False)
        self._assert_job_deduplication_check()

    @simple_layout('layouts/job-dedup-true.yaml')
    def test_job_deduplication_check_true(self):
        self._test_job_deduplication_check()
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self._assert_job_deduplication_check()

    @simple_layout('layouts/job-dedup-false.yaml')
    def test_job_deduplication_check_false(self):
        self._test_job_deduplication_check()
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/02/2/1'),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is not deduplicated, though it would be under auto
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/01/1/1'),
        ], ordered=False)
        self._assert_job_deduplication_check()

    @simple_layout('layouts/job-dedup-file-filters.yaml')
    def test_job_deduplication_check_file_filters(self):
        files_a = {
            "irrelevant/file": "deadbeef",
        }
        files_b = {
            "relevant/file": "decafbad",
        }
        self._test_job_deduplication_check(files_a, files_b)
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self._assert_job_deduplication_check()

    @simple_layout('layouts/job-dedup-empty-nodeset.yaml')
    def test_job_deduplication_check_empty_nodeset(self):
        # Make sure that jobs with empty nodesets can still be
        # deduplicated
        self._test_job_deduplication_check()
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        manager = tenant.layout.pipeline_managers["check"]
        pipeline_path = manager.state.getPath()
        all_items = set(self.zk_client.client.get_children(
            f"{pipeline_path}/item"))
        self.assertEqual(len(all_items), 0)

    @simple_layout('layouts/job-dedup-child-jobs.yaml')
    def test_job_deduplication_check_child_jobs(self):
        # Test that child jobs of deduplicated parents are
        # deduplicated, and also that supplying child_jobs to
        # zuul_return filters correctly.  child1-job should not run,
        # but child2 should run and be deduplicated.  This uses auto
        # deduplication.
        self.executor_server.returnData(
            'common-job', 'refs/changes/02/2/1',
            {'zuul': {'child_jobs': ['child2-job']}}
        )
        self.executor_server.returnData(
            'common-job', 'refs/changes/01/1/1',
            {'zuul': {'child_jobs': ['child2-job']}}
        )

        self._test_job_deduplication_check()
        self.assertHistory([
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="child2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            # dict(name="child2-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self._assert_job_deduplication_check()

    @simple_layout('layouts/job-dedup-child-jobs.yaml')
    def test_job_deduplication_check_child_jobs_paused_parent(self):
        # Test that child jobs returned from deduplicated paused
        # parents are deduplicated and correctly skipped.
        # child1-job should not run, but child2 should run and be
        # deduplicated.  This uses auto deduplication.
        self.executor_server.returnData(
            'common-job', 'refs/changes/02/2/1',
            {'zuul': {
                'child_jobs': ['child2-job'],
                'pause': True,
            }}
        )
        self.executor_server.returnData(
            'common-job', 'refs/changes/01/1/1',
            {'zuul': {
                'child_jobs': ['child2-job'],
                'pause': True,
            }}
        )

        self._test_job_deduplication_check()
        self.assertHistory([
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="child2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            # dict(name="child2-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self._assert_job_deduplication_check()

    @simple_layout('layouts/job-dedup-mismatched-child-jobs.yaml')
    def test_job_deduplication_check_mismatched_child_jobs(self):
        # Test that a parent job with different child jobs is
        # deduplicated.  This uses auto-deduplication.
        self._test_job_deduplication_check()
        self.assertHistory([
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="child1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="child2-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self._assert_job_deduplication_check()

    @simple_layout('layouts/job-dedup-child-of-diff-parent.yaml')
    def test_job_deduplication_check_child_of_diff_parent_diff_data(self):
        # The common job is forced to not deduplicate, and the child
        # job is deduplicated.  The child job treats each of the
        # common jobs as a parent.
        self.executor_server.returnData(
            'common-job', 'refs/changes/02/2/1',
            {'foo': 'a'}
        )
        self.executor_server.returnData(
            'common-job', 'refs/changes/01/1/1',
            {'bar': 'b'}
        )
        self._test_job_deduplication_check()
        self.assertHistory([
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/01/1/1'),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/02/2/1'),
            dict(name="child-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/01/1/1'),
        ], ordered=False)
        # child-job for 2 is deduplicated into 1 (the triggering change).
        self._assert_job_deduplication_check()
        job = self.getJobFromHistory('child-job')
        self.assertEqual({'foo': 'a', 'bar': 'b'},
                         job.parameters['parent_data'])

    @simple_layout('layouts/job-dedup-paused-parent.yaml')
    def test_job_deduplication_check_paused_parent(self):
        # Pause a parent job
        # Ensure it waits for all children before continuing

        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )
        self.executor_server.returnData(
            'common-job', A,
            {'zuul': {'pause': True}}
        )
        self.executor_server.returnData(
            'common-job', B,
            {'zuul': {'pause': True}}
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.executor_server.release('common-job')
        self.waitUntilSettled()
        self.executor_server.release('project1-job')
        self.waitUntilSettled()

        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)

        self.assertEqual(len(self.builds), 2)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self._assert_job_deduplication_check()

    @simple_layout('layouts/job-dedup-auto-shared.yaml')
    def test_job_deduplication_check_failed_node_request(self):
        # Pause nodepool so we can fail the node request later
        self.fake_nodepool.pause()

        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Fail the node request and unpause
        items = self.getAllItems('tenant-one', 'check')
        job = items[0].current_build_set.job_graph.getJob(
            'common-job', items[0].changes[0].cache_key)
        for req in self.fake_nodepool.getNodeRequests():
            if req['requestor_data']['job_uuid'] == job.uuid:
                self.fake_nodepool.addFailRequest(req)

        self.fake_nodepool.unpause()
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(B.data['status'], 'NEW')
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 3)

    @simple_layout('layouts/job-dedup-auto-shared.yaml')
    def test_job_deduplication_check_failed_job(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.executor_server.failJob("common-job", A)

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # If we don't make sure these jobs finish first, then one of
        # the items may complete before the other and cause Zuul to
        # abort the project*-job on the other item (with a "bundle
        # failed to merge" error).
        self.waitUntilSettled()
        self.executor_server.release('project1-job')
        self.executor_server.release('project2-job')
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(B.data['status'], 'NEW')
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="FAILURE", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 3)

    @simple_layout('layouts/job-dedup-false.yaml')
    def test_job_deduplication_check_false_failed_job(self):
        # Test that if we are *not* deduplicating jobs, we don't
        # duplicate the result on two different builds.  The way we
        # check that is to retry the common-job between two changes,
        # but only once, and only on one change.  The other change
        # should be unaffected.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        for build in self.builds:
            if build.name == 'common-job' and build.project == 'org/project1':
                break
        else:
            raise Exception("Unable to find build")
        build.should_retry = True

        self.executor_server.release('project1-job')
        self.executor_server.release('project2-job')
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(B.data['status'], 'NEW')
        self.assertHistory([
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result=None, changes="2,1 1,1",
                 ref='refs/changes/01/1/1'),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/01/1/1'),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/02/2/1'),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 5)

    @simple_layout('layouts/job-dedup-auto-shared.yaml')
    def test_job_deduplication_check_multi_scheduler(self):
        # Test that a second scheduler can correctly refresh
        # deduplicated builds
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        app = self.createScheduler()
        app.start()
        self.assertEqual(len(self.scheds), 2)

        # Hold the lock on the first scheduler so that only the second
        # will act.
        with self.scheds.first.sched.run_handler_lock:
            self.executor_server.hold_jobs_in_build = False
            self.executor_server.release()
            self.waitUntilSettled(matcher=[app])

        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)

    @simple_layout('layouts/job-dedup-multi-sched-complete.yaml')
    def test_job_deduplication_check_multi_scheduler_complete(self):
        # Test that a second scheduler can correctly refresh
        # deduplicated builds after the first scheduler has completed
        # the builds for one item.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.executor_server.release('common-job')
        self.executor_server.release('project1-job')
        self.waitUntilSettled()

        app = self.createScheduler()
        app.start()
        self.assertEqual(len(self.scheds), 2)

        # Hold the lock on the first scheduler so that only the second
        # will act.
        with self.scheds.first.sched.run_handler_lock:
            self.executor_server.hold_jobs_in_build = False

            # Make sure we go through an extra pipeline cycle so that
            # the cleanup method runs.
            self.executor_server.release('project2-job2')
            self.waitUntilSettled(matcher=[app])

            self.executor_server.release()
            self.waitUntilSettled(matcher=[app])

        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job2", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)

    @simple_layout('layouts/job-dedup-multi-sched-complete.yaml')
    def test_job_deduplication_check_multi_scheduler_complete2(self):
        # Test that a second scheduler can correctly refresh
        # deduplicated builds after the first scheduler has completed
        # the builds for one item.

        # This is similar to the previous test but starts the second
        # scheduler slightly later.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.executor_server.release('common-job')
        self.waitUntilSettled()

        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.executor_server.release('project1-job')
        self.waitUntilSettled()

        app = self.createScheduler()
        app.start()
        self.assertEqual(len(self.scheds), 2)

        # Hold the lock on the first scheduler so that only the second
        # will act.
        with self.scheds.first.sched.run_handler_lock:
            self.executor_server.hold_jobs_in_build = False

            self.executor_server.release('project2-job2')
            self.waitUntilSettled(matcher=[app])

            self.executor_server.release()
            self.waitUntilSettled(matcher=[app])

        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job2", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)

    @simple_layout('layouts/job-dedup-noop.yaml')
    def test_job_deduplication_check_noop(self):
        # Test that we deduplicate noop (there's no good reason not
        # to do so)
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        # It's tricky to get info about a noop build, but the jobs in
        # the report have the build UUID, so we make sure it's
        # the same and we only have one noop job.
        a_noop = [l for l in A.messages[-1].split('\n') if 'noop' in l]
        b_noop = [l for l in B.messages[-1].split('\n') if 'noop' in l]
        self.assertEqual(a_noop, b_noop)
        self.assertEqual(len(a_noop), 1)

    @simple_layout('layouts/job-dedup-retry.yaml')
    def test_job_deduplication_check_retry(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.executor_server.retryJob('common-job', A)

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # There should be exactly 3 runs of the job (not 6)
            dict(name="common-job", result=None, changes="2,1 1,1"),
            dict(name="common-job", result=None, changes="2,1 1,1"),
            dict(name="common-job", result=None, changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 5)

    @simple_layout('layouts/job-dedup-retry-child.yaml')
    def test_job_deduplication_check_retry_child(self):
        # This tests retrying a paused build (simulating an executor restart)
        # See test_data_return_child_from_retried_paused_job
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.executor_server.returnData(
            'parent-job', A,
            {'zuul': {'pause': True}}
        )
        self.executor_server.returnData(
            'parent-job', B,
            {'zuul': {'pause': True}}
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.executor_server.release('parent-job')
        self.waitUntilSettled("till job is paused")

        paused_job = self.builds[0]
        self.assertTrue(paused_job.paused)

        # Stop the job worker to simulate an executor restart
        for job_worker in self.executor_server.job_workers.values():
            if job_worker.build_request.uuid == paused_job.uuid:
                job_worker.stop()
        self.waitUntilSettled("stop job worker")

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled("all jobs are done")
        # The "pause" job might be paused during the waitUntilSettled
        # call and appear settled; it should automatically resume
        # though, so just wait for it.
        for x in iterate_timeout(60, 'paused job'):
            if not self.builds:
                break
        self.waitUntilSettled()

        self.assertHistory([
            dict(name="parent-job", result="ABORTED", changes="2,1 1,1"),
            dict(name="project1-job", result="ABORTED", changes="2,1 1,1"),
            dict(name="project2-job", result="ABORTED", changes="2,1 1,1"),
            dict(name="parent-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 6)

    @simple_layout('layouts/job-dedup-parent-data.yaml')
    def test_job_deduplication_check_parent_data(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        # The parent job returns data
        self.executor_server.returnData(
            'parent-job', A,
            {'zuul':
             {'artifacts': [
                 {'name': 'image',
                  'url': 'http://example.com/image',
                  'metadata': {
                      'type': 'container_image'
                  }},
             ]}}
        )
        self.executor_server.returnData(
            'parent-job', B,
            {'zuul':
             {'artifacts': [
                 {'name': 'image',
                  'url': 'http://example.com/image',
                  'metadata': {
                      'type': 'container_image'
                  }},
             ]}}
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name="parent-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # Only one run of the common job since it's the same
            dict(name="common-child-job", result="SUCCESS", changes="2,1 1,1"),
            # The forked job depends on different parents
            # so it should run twice
            dict(name="forked-child-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="forked-child-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 6)

    @simple_layout('layouts/job-dedup-parent.yaml')
    def test_job_deduplication_check_parent_data2(self):
        # This is similar to the parent_data test, but it waits longer
        # to add the second change so that the deduplication of both
        # parent and child happen at the same time.  This verifies
        # that when parent data is supplied to child jobs
        # deduplication still works.

        # This has no equivalent for dependent pipelines since it's
        # not possible to delay enqueing the second change.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        self.executor_server.returnData(
            'common-job', A,
            {'zuul':
             {'artifacts': [
                 {'name': 'image',
                  'url': 'http://example.com/image',
                  'metadata': {
                      'type': 'container_image'
                  }},
             ]}}
        )

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.executor_server.release('common-job')
        self.waitUntilSettled()
        self.executor_server.release('child-job')
        self.waitUntilSettled()

        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/01/1/1'),
            dict(name="child-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/01/1/1'),
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1",
                 ref='refs/changes/01/1/1'),
        ], ordered=False)
        # All jobs for 2 are deduplicated into 1 (the triggering change).
        self._assert_job_deduplication_check()

    def _test_job_deduplication_check_semaphore(self):
        "Test semaphores with max=1 (mutex) and get resources first"
        self.executor_server.hold_jobs_in_build = True

        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(
            len(tenant.semaphore_handler.semaphoreHolders("test-semaphore")),
            0)

        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        # A <-> B
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(
            len(tenant.semaphore_handler.semaphoreHolders("test-semaphore")),
            1)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name="project1-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project2-job", result="SUCCESS", changes="2,1 1,1"),
            # This is deduplicated
            # dict(name="common-job", result="SUCCESS", changes="2,1 1,1"),
        ], ordered=False)
        self.assertEqual(len(self.fake_nodepool.history), 3)
        self.assertEqual(
            len(tenant.semaphore_handler.semaphoreHolders("test-semaphore")),
            0)

    @simple_layout('layouts/job-dedup-semaphore.yaml')
    def test_job_deduplication_check_semaphore(self):
        self._test_job_deduplication_check_semaphore()

    @simple_layout('layouts/job-dedup-semaphore-first.yaml')
    def test_job_deduplication_check_semaphore_resources_first(self):
        self._test_job_deduplication_check_semaphore()

    # End check tests

    @gerrit_config(submit_whole_topic=True)
    def test_submitted_together(self):
        A = self.fake_gerrit.addFakeChange('org/project1', "master", "A",
                                           topic='test-topic')
        B = self.fake_gerrit.addFakeChange('org/project2', "master", "B",
                                           topic='test-topic')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "1")

        # We're about to add approvals to changes without adding the
        # triggering events to Zuul, so that we can be sure that it is
        # enqueing the changes based on dependencies, not because of
        # triggering events.  Since it will have the changes cached
        # already (without approvals), we need to clear the cache
        # first.
        for connection in self.scheds.first.connections.connections.values():
            connection.maintainCache([], max_age=0)

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        A.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(B.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 3)
        self.assertEqual(B.reported, 3)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")

    @gerrit_config(submit_whole_topic=True)
    def test_submitted_together_storm(self):
        # Test that if many changes are uploaded with the same topic,
        # we handle queries efficiently.
        self.waitUntilSettled()
        A = self.fake_gerrit.addFakeChange('org/project', "master", "A",
                                           topic='test-topic')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        B = self.fake_gerrit.addFakeChange('org/project1', "master", "B",
                                           topic='test-topic')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        C = self.fake_gerrit.addFakeChange('org/project2', "master", "C",
                                           topic='test-topic')
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Output all the queries seen for debugging
        for q in self.fake_gerrit.api_calls:
            self.log.debug("Query: %s", q)

        gets = [q[1] for q in self.fake_gerrit.api_calls if q[0] == 'GET']
        counters = Counter()
        for q in gets:
            parts = q.split('/')[2:]
            if len(parts) > 2 and parts[2] == 'revisions':
                parts.pop(3)
            if 'q=message' in parts[1]:
                parts[1] = 'message'
            counters[tuple(parts)] += 1
        # Ensure that we don't run these queries more than once for each change
        qstring = ('o=DETAILED_ACCOUNTS&o=CURRENT_REVISION&'
                   'o=CURRENT_COMMIT&o=CURRENT_FILES&o=LABELS&'
                   'o=DETAILED_LABELS&o=ALL_REVISIONS')
        self.assertEqual(1, counters[('changes', f'1?{qstring}')])
        self.assertEqual(1, counters[('changes', f'2?{qstring}')])
        self.assertEqual(1, counters[('changes', f'3?{qstring}')])
        self.assertEqual(1, counters[('changes', '1', 'revisions', 'related')])
        self.assertEqual(1, counters[('changes', '2', 'revisions', 'related')])
        self.assertEqual(1, counters[('changes', '3', 'revisions', 'related')])
        self.assertEqual(1, counters[
            ('changes', '1', 'revisions', 'files?parent=1')])
        self.assertEqual(1, counters[
            ('changes', '2', 'revisions', 'files?parent=1')])
        self.assertEqual(1, counters[
            ('changes', '3', 'revisions', 'files?parent=1')])
        self.assertEqual(3, counters[('changes', 'message')])
        # These queries are no longer used
        self.assertEqual(0, counters[('changes', '1', 'submitted_together')])
        self.assertEqual(0, counters[('changes', '2', 'submitted_together')])
        self.assertEqual(0, counters[('changes', '3', 'submitted_together')])
        # This query happens once for each event in the scheduler.
        qstring = ('?n=500&o=CURRENT_REVISION&o=CURRENT_COMMIT&'
                   'q=status%3Aopen%20topic%3A%22test-topic%22')
        self.assertEqual(3, counters[('changes', qstring)])
        self.assertHistory([
            dict(name="project-job", changes="1,1"),

            dict(name="project-job", changes="1,1 2,1"),
            dict(name="project1-job", changes="1,1 2,1"),
            dict(name="project-vars-job", changes="1,1 2,1"),

            dict(name="project-job", changes="2,1 1,1 3,1"),
            dict(name="project1-job", changes="2,1 1,1 3,1"),
            dict(name="project-vars-job", changes="2,1 1,1 3,1"),
            dict(name="project2-job", changes="2,1 1,1 3,1"),
        ], ordered=False)

    @gerrit_config(submit_whole_topic=True)
    def test_submitted_together_storm_fast(self):
        # Test that if many changes are uploaded with the same topic,
        # we handle queries efficiently.

        def waitForEvents():
            for x in iterate_timeout(60, 'empty event queue'):
                if not len(self.fake_gerrit.event_queue):
                    return
            self.log.debug("Empty event queue")

        # This mimics the changes being uploaded in rapid succession.
        self.waitUntilSettled()
        with self.scheds.first.sched.run_handler_lock:
            A = self.fake_gerrit.addFakeChange('org/project', "master", "A",
                                               topic='test-topic')
            self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
            waitForEvents()

            B = self.fake_gerrit.addFakeChange('org/project1', "master", "B",
                                               topic='test-topic')
            self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
            waitForEvents()

            C = self.fake_gerrit.addFakeChange('org/project2', "master", "C",
                                               topic='test-topic')
            self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
            waitForEvents()
        self.waitUntilSettled()

        # Output all the queries seen for debugging
        for q in self.fake_gerrit.api_calls:
            self.log.debug("Query: %s", q)

        gets = [q[1] for q in self.fake_gerrit.api_calls if q[0] == 'GET']
        counters = Counter()
        for q in gets:
            parts = q.split('/')[2:]
            if len(parts) > 2 and parts[2] == 'revisions':
                parts.pop(3)
            if 'q=message' in parts[1]:
                parts[1] = 'message'
            counters[tuple(parts)] += 1
        # Ensure that we don't run these queries more than once for each change
        qstring = ('o=DETAILED_ACCOUNTS&o=CURRENT_REVISION&'
                   'o=CURRENT_COMMIT&o=CURRENT_FILES&o=LABELS&'
                   'o=DETAILED_LABELS&o=ALL_REVISIONS')
        self.assertEqual(1, counters[('changes', f'1?{qstring}')])
        self.assertEqual(1, counters[('changes', f'2?{qstring}')])
        self.assertEqual(1, counters[('changes', f'3?{qstring}')])
        self.assertEqual(1, counters[('changes', '1', 'revisions', 'related')])
        self.assertEqual(1, counters[('changes', '2', 'revisions', 'related')])
        self.assertEqual(1, counters[('changes', '3', 'revisions', 'related')])
        self.assertEqual(1, counters[
            ('changes', '1', 'revisions', 'files?parent=1')])
        self.assertEqual(1, counters[
            ('changes', '2', 'revisions', 'files?parent=1')])
        self.assertEqual(1, counters[
            ('changes', '3', 'revisions', 'files?parent=1')])
        self.assertEqual(3, counters[('changes', 'message')])
        # These queries are no longer used
        self.assertEqual(0, counters[('changes', '1', 'submitted_together')])
        self.assertEqual(0, counters[('changes', '2', 'submitted_together')])
        self.assertEqual(0, counters[('changes', '3', 'submitted_together')])
        # This query happens once for each event (but is cached).
        # * A+B+C: 3x scheduler, 0x pipeline
        qstring = ('?n=500&o=CURRENT_REVISION&o=CURRENT_COMMIT&'
                   'q=status%3Aopen%20topic%3A%22test-topic%22')
        self.assertEqual(1, counters[('changes', qstring)])
        self.assertHistory([
            dict(name="project-job", changes="3,1 2,1 1,1"),
            dict(name="project1-job", changes="3,1 2,1 1,1"),
            dict(name="project-vars-job", changes="3,1 2,1 1,1"),
            dict(name="project2-job", changes="3,1 2,1 1,1"),
        ], ordered=False)

    @gerrit_config(submit_whole_topic=True)
    def test_submitted_together_git(self):
        A = self.fake_gerrit.addFakeChange('org/project1', "master", "A")
        B = self.fake_gerrit.addFakeChange('org/project1', "master", "B")
        C = self.fake_gerrit.addFakeChange('org/project1', "master", "C")
        D = self.fake_gerrit.addFakeChange('org/project1', "master", "D")
        E = self.fake_gerrit.addFakeChange('org/project1', "master", "E")
        F = self.fake_gerrit.addFakeChange('org/project1', "master", "F")
        G = self.fake_gerrit.addFakeChange('org/project1', "master", "G")
        G.setDependsOn(F, 1)
        F.setDependsOn(E, 1)
        E.setDependsOn(D, 1)
        D.setDependsOn(C, 1)
        C.setDependsOn(B, 1)
        B.setDependsOn(A, 1)

        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(C.patchsets[-1]["approvals"]), 1)
        self.assertEqual(C.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(C.patchsets[-1]["approvals"][0]["value"], "1")
        self.assertEqual(A.queried, 1)
        self.assertEqual(B.queried, 1)
        self.assertEqual(C.queried, 1)
        self.assertEqual(D.queried, 1)
        self.assertEqual(E.queried, 1)
        self.assertEqual(F.queried, 1)
        self.assertEqual(G.queried, 1)
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS",
                 changes="1,1 2,1 3,1"),
            dict(name="project-vars-job", result="SUCCESS",
                 changes="1,1 2,1 3,1"),
        ], ordered=False)

    @gerrit_config(submit_whole_topic=True)
    def test_submitted_together_git_topic(self):
        A = self.fake_gerrit.addFakeChange('org/project1', "master", "A",
                                           topic='test-topic')
        B = self.fake_gerrit.addFakeChange('org/project1', "master", "B",
                                           topic='test-topic')
        C = self.fake_gerrit.addFakeChange('org/project1', "master", "C",
                                           topic='test-topic')
        D = self.fake_gerrit.addFakeChange('org/project1', "master", "D",
                                           topic='test-topic')
        E = self.fake_gerrit.addFakeChange('org/project1', "master", "E",
                                           topic='test-topic')
        F = self.fake_gerrit.addFakeChange('org/project1', "master", "F",
                                           topic='test-topic')
        G = self.fake_gerrit.addFakeChange('org/project1', "master", "G",
                                           topic='test-topic')
        G.setDependsOn(F, 1)
        F.setDependsOn(E, 1)
        E.setDependsOn(D, 1)
        D.setDependsOn(C, 1)
        C.setDependsOn(B, 1)
        B.setDependsOn(A, 1)

        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(C.patchsets[-1]["approvals"]), 1)
        self.assertEqual(C.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(C.patchsets[-1]["approvals"][0]["value"], "1")
        self.assertEqual(A.queried, 2)
        self.assertEqual(B.queried, 2)
        self.assertEqual(C.queried, 2)
        self.assertEqual(D.queried, 2)
        self.assertEqual(E.queried, 2)
        self.assertEqual(F.queried, 2)
        self.assertEqual(G.queried, 2)
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS",
                 changes="1,1 2,1 3,1 4,1 5,1 6,1 7,1",
                 ref="refs/changes/01/1/1"),
            dict(name="project1-job", result="SUCCESS",
                 changes="1,1 2,1 3,1 4,1 5,1 6,1 7,1",
                 ref="refs/changes/02/2/1"),
            dict(name="project1-job", result="SUCCESS",
                 changes="1,1 2,1 3,1 4,1 5,1 6,1 7,1",
                 ref="refs/changes/03/3/1"),
            dict(name="project1-job", result="SUCCESS",
                 changes="1,1 2,1 3,1 4,1 5,1 6,1 7,1",
                 ref="refs/changes/04/4/1"),
            dict(name="project1-job", result="SUCCESS",
                 changes="1,1 2,1 3,1 4,1 5,1 6,1 7,1",
                 ref="refs/changes/05/5/1"),
            dict(name="project1-job", result="SUCCESS",
                 changes="1,1 2,1 3,1 4,1 5,1 6,1 7,1",
                 ref="refs/changes/06/6/1"),
            dict(name="project1-job", result="SUCCESS",
                 changes="1,1 2,1 3,1 4,1 5,1 6,1 7,1",
                 ref="refs/changes/07/7/1"),
            dict(name="project-vars-job", result="SUCCESS",
                 changes="1,1 2,1 3,1 4,1 5,1 6,1 7,1",
                 ref="refs/changes/01/1/1"),
            dict(name="project-vars-job", result="SUCCESS",
                 changes="1,1 2,1 3,1 4,1 5,1 6,1 7,1",
                 ref="refs/changes/02/2/1"),
            dict(name="project-vars-job", result="SUCCESS",
                 changes="1,1 2,1 3,1 4,1 5,1 6,1 7,1",
                 ref="refs/changes/03/3/1"),
            dict(name="project-vars-job", result="SUCCESS",
                 changes="1,1 2,1 3,1 4,1 5,1 6,1 7,1",
                 ref="refs/changes/04/4/1"),
            dict(name="project-vars-job", result="SUCCESS",
                 changes="1,1 2,1 3,1 4,1 5,1 6,1 7,1",
                 ref="refs/changes/05/5/1"),
            dict(name="project-vars-job", result="SUCCESS",
                 changes="1,1 2,1 3,1 4,1 5,1 6,1 7,1",
                 ref="refs/changes/06/6/1"),
            dict(name="project-vars-job", result="SUCCESS",
                 changes="1,1 2,1 3,1 4,1 5,1 6,1 7,1",
                 ref="refs/changes/07/7/1"),
        ], ordered=False)

    @simple_layout('layouts/submitted-together-per-branch.yaml')
    @gerrit_config(submit_whole_topic=True)
    def test_submitted_together_per_branch(self):
        self.create_branch('org/project2', 'stable/foo')
        A = self.fake_gerrit.addFakeChange('org/project1', "master", "A",
                                           topic='test-topic')
        B = self.fake_gerrit.addFakeChange('org/project2', "stable/foo", "B",
                                           topic='test-topic')

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        A.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(B.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 0)
        self.assertEqual(B.reported, 1)
        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(B.data["status"], "NEW")
        self.assertIn("does not share a change queue", B.messages[-1])

    @simple_layout('layouts/deps-by-topic.yaml')
    def test_deps_by_topic(self):
        A = self.fake_gerrit.addFakeChange('org/project1', "master", "A",
                                           topic='test topic')
        B = self.fake_gerrit.addFakeChange('org/project2', "master", "B",
                                           topic='test topic')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "1")

        # We're about to add approvals to changes without adding the
        # triggering events to Zuul, so that we can be sure that it is
        # enqueing the changes based on dependencies, not because of
        # triggering events.  Since it will have the changes cached
        # already (without approvals), we need to clear the cache
        # first.
        for connection in self.scheds.first.connections.connections.values():
            connection.maintainCache([], max_age=0)

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        A.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(B.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 3)
        self.assertEqual(B.reported, 3)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")

    @simple_layout('layouts/deps-by-topic.yaml')
    def test_deps_by_topic_git_needs(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', "master", "A",
                                           topic='test-topic')
        B = self.fake_gerrit.addFakeChange('org/project2', "master", "B",
                                           topic='test-topic')
        C = self.fake_gerrit.addFakeChange('org/project2', "master", "C",
                                           topic='other-topic')
        D = self.fake_gerrit.addFakeChange('org/project1', "master", "D",
                                           topic='other-topic')

        # Git level dependency between B and C
        B.setDependsOn(C, 1)

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(D.getPatchsetCreatedEvent(1))
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(C.patchsets[-1]["approvals"]), 1)
        self.assertEqual(C.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(C.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(D.patchsets[-1]["approvals"]), 1)
        self.assertEqual(D.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(D.patchsets[-1]["approvals"][0]["value"], "1")

        # We're about to add approvals to changes without adding the
        # triggering events to Zuul, so that we can be sure that it is
        # enqueing the changes based on dependencies, not because of
        # triggering events.  Since it will have the changes cached
        # already (without approvals), we need to clear the cache
        # first.
        for connection in self.scheds.first.connections.connections.values():
            connection.maintainCache([], max_age=0)

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        C.addApproval("Code-Review", 2)
        D.addApproval("Code-Review", 2)
        A.addApproval("Approved", 1)
        C.addApproval("Approved", 1)
        D.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(B.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 3)
        self.assertEqual(B.reported, 3)
        self.assertEqual(C.reported, 3)
        self.assertEqual(D.reported, 3)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")
        self.assertEqual(C.data["status"], "MERGED")
        self.assertEqual(D.data["status"], "MERGED")

    @simple_layout('layouts/deps-by-topic.yaml')
    def test_deps_by_topic_git_needs_outdated_patchset(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', "master", "A",
                                           topic='test-topic')
        B = self.fake_gerrit.addFakeChange('org/project1', "master", "B",
                                           topic='other-topic')
        C = self.fake_gerrit.addFakeChange('org/project2', "master", "C",
                                           topic='other-topic')
        D = self.fake_gerrit.addFakeChange('org/project2', "master", "D",
                                           topic='test-topic')

        B.addPatchset()
        D.addPatchset()
        # Git level dependency between A and B + C and D on outdated
        # patchset.
        A.setDependsOn(B, 1)
        C.setDependsOn(D, 1)

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(2))
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(D.getPatchsetCreatedEvent(2))
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        # This used to run jobs with some very unlikely changes
        # merged.  That passed the test because we didn't actually get
        # a ref for the outdated commits, so when the merger merged
        # them, that was a noop.  Since we now have correct refs for
        # the outdated changes, we hit a merge conflict, which is a
        # reasonable and likely error.  It's not likely that we can
        # make this test run a job with correct data.
        # self.assertHistory([
        #     dict(name="check-job", result="SUCCESS",
        #          changes="4,2 4,1 3,1 2,2 2,1 1,1"),
        #     dict(name="check-job", result="SUCCESS",
        #          changes="4,2 2,2 2,1 1,1 4,1 3,1"),
        # ], ordered=False)
        self.assertHistory([], ordered=False)

    @simple_layout('layouts/deps-by-topic.yaml')
    def test_deps_by_topic_new_patchset(self):
        # Make sure that we correctly update the change cache on new
        # patchsets.
        A = self.fake_gerrit.addFakeChange('org/project1', "master", "A",
                                           topic='test-topic')
        B = self.fake_gerrit.addFakeChange('org/project2', "master", "B",
                                           topic='test-topic')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertHistory([
            dict(name="check-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="check-job", result="SUCCESS", changes="2,1 1,1"),
        ])

        A.addPatchset()
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(2))
        self.waitUntilSettled()

        self.assertHistory([
            # Original check run
            dict(name="check-job", result="SUCCESS", changes="2,1 1,1"),
            dict(name="check-job", result="SUCCESS", changes="2,1 1,1"),
            # Second check run
            dict(name="check-job", result="SUCCESS", changes="2,1 1,2"),
            dict(name="check-job", result="SUCCESS", changes="2,1 1,2"),
        ], ordered=False)

    def test_deps_by_topic_multi_tenant(self):
        A = self.fake_gerrit.addFakeChange('org/project5', "master", "A",
                                           topic='test-topic')
        B = self.fake_gerrit.addFakeChange('org/project6', "master", "B",
                                           topic='test-topic')

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "1")

        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "1")

        # We're about to add approvals to changes without adding the
        # triggering events to Zuul, so that we can be sure that it is
        # enqueuing the changes based on dependencies, not because of
        # triggering events.  Since it will have the changes cached
        # already (without approvals), we need to clear the cache
        # first.
        for connection in self.scheds.first.connections.connections.values():
            connection.maintainCache([], max_age=0)

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        A.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(B.addApproval("Approved", 1))
        self.waitUntilSettled()

        self.assertEqual(A.reported, 4)
        self.assertEqual(B.reported, 4)
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")

        self.assertHistory([
            # Check
            dict(name="project5-job-t1", result="SUCCESS", changes="1,1"),
            dict(name="project6-job-t1", result="SUCCESS", changes="2,1"),
            dict(name="project5-job-t2", result="SUCCESS", changes="2,1 1,1"),
            dict(name="project6-job-t2", result="SUCCESS", changes="2,1 1,1"),
            # Gate
            dict(name="project5-job-t2", result="SUCCESS", changes="1,1 2,1"),
            dict(name="project6-job-t2", result="SUCCESS", changes="1,1 2,1"),
        ], ordered=False)

    def test_dependency_refresh(self):
        # Test that when two changes are put into a cycle, the
        # dependencies are refreshed and items already in pipelines
        # are updated.
        self.executor_server.hold_jobs_in_build = True

        # This simulates the typical workflow where a developer only
        # knows the change id of changes one at a time.
        # The first change:
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled("Stage 1")

        # Now that it has been uploaded, upload the second change and
        # point it at the first.
        # B -> A
        B = self.fake_gerrit.addFakeChange("org/project", "master", "B")
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled("Stage 2")

        # Now that the second change is known, update the first change
        # B <-> A
        A.addPatchset()
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(2))
        self.waitUntilSettled("Stage 3")

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled("Stage 4")

        self.assertHistory([
            dict(name="project-job", result="ABORTED", changes="1,1"),
            dict(name="project-job", result="ABORTED", changes="1,1 2,1"),
            dict(name="project-job", result="SUCCESS", changes="2,1 1,2"),
            dict(name="project-job", result="SUCCESS", changes="2,1 1,2"),
        ], ordered=False)

    @simple_layout('layouts/deps-by-topic.yaml')
    def test_dependency_refresh_by_topic_check(self):
        # Test that when two changes are put into a cycle, the
        # dependencies are refreshed and items already in pipelines
        # are updated.
        self.executor_server.hold_jobs_in_build = True

        # This simulates the typical workflow where a developer
        # uploads changes one at a time.
        # The first change:
        A = self.fake_gerrit.addFakeChange('org/project1', "master", "A",
                                           topic='test-topic')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Now that it has been uploaded, upload the second change
        # in the same topic.
        B = self.fake_gerrit.addFakeChange('org/project2', "master", "B",
                                           topic='test-topic')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name="check-job", result="ABORTED", changes="1,1"),
            dict(name="check-job", result="SUCCESS", changes="1,1 2,1",
                 ref="refs/changes/01/1/1"),
            dict(name="check-job", result="SUCCESS", changes="1,1 2,1",
                 ref="refs/changes/02/2/1"),
        ], ordered=False)

    @simple_layout('layouts/deps-by-topic.yaml')
    def test_dependency_refresh_by_topic_gate(self):
        # Test that when two changes are put into a cycle, the
        # dependencies are refreshed and items already in pipelines
        # are updated.
        self.executor_server.hold_jobs_in_build = True

        # This simulates a workflow where a developer adds a change to
        # a cycle already in gate.
        A = self.fake_gerrit.addFakeChange('org/project1', "master", "A",
                                           topic='test-topic')
        B = self.fake_gerrit.addFakeChange('org/project2', "master", "B",
                                           topic='test-topic')
        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        A.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(B.addApproval("Approved", 1))
        self.waitUntilSettled()

        # Add a new change to the cycle.
        C = self.fake_gerrit.addFakeChange('org/project1', "master", "C",
                                           topic='test-topic')
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        # At the end of this process, the gate jobs should be aborted
        # because the new dpendency showed up.
        self.assertEqual(A.data["status"], "NEW")
        self.assertEqual(B.data["status"], "NEW")
        self.assertEqual(C.data["status"], "NEW")
        self.assertHistory([
            dict(name="gate-job", result="ABORTED", changes="1,1 2,1",
                 ref="refs/changes/01/1/1"),
            dict(name="gate-job", result="ABORTED", changes="1,1 2,1",
                 ref="refs/changes/02/2/1"),
            dict(name="check-job", result="SUCCESS", changes="2,1 1,1 3,1",
                 ref="refs/changes/02/2/1"),
            dict(name="check-job", result="SUCCESS", changes="2,1 1,1 3,1",
                 ref="refs/changes/03/3/1"),
            # check-job for change 1 is deduplicated into change 3
            # because they are the same repo; 3 is the triggering change.
        ], ordered=False)

    def test_dependency_refresh_config_error(self):
        # Test that when two changes are put into a cycle, the
        # dependencies are refreshed and items already in pipelines
        # are updated.
        self.executor_server.hold_jobs_in_build = True

        # This simulates the typical workflow where a developer only
        # knows the change id of changes one at a time.
        # The first change:
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Now that it has been uploaded, upload the second change and
        # point it at the first.
        # B -> A
        B = self.fake_gerrit.addFakeChange("org/project", "master", "B")
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Enforce a broken project pipeline config. The project stanza
        # for org/project references an non-existing project-template
        # which triggers the corner case we are looking for in this test.
        self.commitConfigUpdate(
            'common-config',
            'config/circular-dependencies/zuul-reconfiguration-broken.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        # Now that the second change is known, update the first change
        # B <-> A
        A.addPatchset()
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(2))
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        # Since the tenant configuration got broken, zuul won't attempt to
        # re-enqueue those changes.
        self.assertHistory([
            dict(name="project-job", result="ABORTED", changes="1,1"),
            dict(name="project-job", result="ABORTED", changes="1,1 2,1"),
        ], ordered=False)

    def test_circular_dependency_config_error(self):
        # Use an error that does not leave a file comment to better
        # exercise the change matching function.
        use_job = textwrap.dedent(
            """
            - foo:
              project:
            """)
        A = self.fake_gerrit.addFakeChange("org/project", "master", "A")
        B = self.fake_gerrit.addFakeChange("org/project1", "master", "B",
                                           files={"zuul.yaml": use_job})

        # A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )

        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([])
        self.assertEqual(len(A.patchsets[-1]["approvals"]), 1)
        self.assertEqual(A.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(A.patchsets[-1]["approvals"][0]["value"], "-1")
        self.assertEqual(len(B.patchsets[-1]["approvals"]), 1)
        self.assertEqual(B.patchsets[-1]["approvals"][0]["type"], "Verified")
        self.assertEqual(B.patchsets[-1]["approvals"][0]["value"], "-1")
        # Only report the error message to B
        self.assertNotIn("more than one key", A.messages[-1])
        self.assertIn("more than one key", B.messages[-1])
        # Indicate that B has an error
        self.assertIn("/2 (config error)", A.messages[-1])
        self.assertIn("/2 (config error)", B.messages[-1])

    @gerrit_config(submit_whole_topic=True)
    def test_abandoned_change(self):
        # Test that we can re-enqueue a topic cycle after abandoning a
        # change (out of band).
        A = self.fake_gerrit.addFakeChange('org/project1', "master", "A",
                                           topic='test-topic')
        B = self.fake_gerrit.addFakeChange('org/project2', "master", "B",
                                           topic='test-topic',
                                           files={'conflict': 'B'})
        X = self.fake_gerrit.addFakeChange('org/project2', "master", "X",
                                           topic='test-topic',
                                           files={'conflict': 'X'})

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        X.addApproval("Code-Review", 2)
        X.addApproval("Approved", 1)
        B.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        X.setAbandoned()
        self.waitUntilSettled("abandoned")

        self.log.debug("add reapproval")
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled("reapproved")

        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")
        self.assertEqual(X.data["status"], "ABANDONED")

    @gerrit_config(submit_whole_topic=True)
    def test_abandoned_change_refresh_changes(self):
        # Test that we can re-enqueue a topic cycle after abandoning a
        # change (out of band).  This adds extra events to refresh all
        # of the changes while the re-enqueue is in progress in order
        # to trigger a race condition.
        A = self.fake_gerrit.addFakeChange('org/project1', "master", "A",
                                           topic='test-topic')
        B = self.fake_gerrit.addFakeChange('org/project2', "master", "B",
                                           topic='test-topic',
                                           files={'conflict': 'B'})
        X = self.fake_gerrit.addFakeChange('org/project2', "master", "X",
                                           topic='test-topic',
                                           files={'conflict': 'X'})

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        X.addApproval("Code-Review", 2)
        X.addApproval("Approved", 1)
        B.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        orig_forward = zuul.scheduler.Scheduler._forward_trigger_event
        stop_event = threading.Event()
        go_event = threading.Event()

        def patched_forward(obj, *args, **kw):
            stop_event.set()
            go_event.wait()
            return orig_forward(obj, *args, **kw)

        self.useFixture(fixtures.MonkeyPatch(
            'zuul.scheduler.Scheduler._forward_trigger_event',
            patched_forward))

        X.setAbandoned()
        X.addApproval("Approved", -1)
        self.waitUntilSettled("abandoned")

        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        stop_event.wait()
        stop_event.clear()
        # The scheduler is waiting to forward the approved event; send
        # another event that refreshes the cache:
        self.fake_gerrit.addEvent(A.getChangeCommentEvent(1, 'testcomment'))
        self.fake_gerrit.addEvent(B.getChangeCommentEvent(1, 'testcomment'))
        self.fake_gerrit.addEvent(X.getChangeCommentEvent(1, 'testcomment'))
        for x in iterate_timeout(30, 'events to be submitted'):
            if len(self.scheds.first.sched.trigger_events['tenant-one']) == 4:
                break
        go_event.set()
        stop_event.wait()
        self.waitUntilSettled("reapproved")

        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")
        self.assertEqual(X.data["status"], "ABANDONED")


class TestGithubCircularDependencies(ZuulTestCase):
    config_file = "zuul-gerrit-github.conf"
    tenant_config_file = "config/circular-dependencies/main.yaml"
    scheduler_count = 1

    def test_cycle_not_ready(self):
        # C is missing the approved label
        A = self.fake_github.openFakePullRequest("gh/project", "master", "A")
        B = self.fake_github.openFakePullRequest("gh/project1", "master", "B")
        C = self.fake_github.openFakePullRequest("gh/project1", "master", "C")
        A.addReview('derp', 'APPROVED')
        B.addReview('derp', 'APPROVED')
        B.addLabel("approved")
        C.addReview('derp', 'APPROVED')

        # A -> B + C (via PR depends)
        # B -> A
        # C -> A
        A.body = "{}\n\nDepends-On: {}\nDepends-On: {}\n".format(
            A.subject, B.url, C.url
        )
        B.body = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.url
        )
        C.body = "{}\n\nDepends-On: {}\n".format(
            C.subject, A.url
        )

        self.fake_github.emitEvent(A.addLabel("approved"))
        self.waitUntilSettled()

        self.assertEqual(len(A.comments), 0)
        self.assertEqual(len(B.comments), 0)
        self.assertEqual(len(C.comments), 0)
        self.assertFalse(A.is_merged)
        self.assertFalse(B.is_merged)
        self.assertFalse(C.is_merged)

    def test_complex_cycle_not_ready(self):
        A = self.fake_github.openFakePullRequest("gh/project", "master", "A")
        B = self.fake_github.openFakePullRequest("gh/project1", "master", "B")
        C = self.fake_github.openFakePullRequest("gh/project1", "master", "C")
        X = self.fake_github.openFakePullRequest("gh/project1", "master", "C")
        Y = self.fake_github.openFakePullRequest("gh/project1", "master", "C")
        A.addReview('derp', 'APPROVED')
        A.addLabel("approved")
        B.addReview('derp', 'APPROVED')
        B.addLabel("approved")
        C.addReview('derp', 'APPROVED')
        Y.addReview('derp', 'APPROVED')
        Y.addLabel("approved")
        X.addReview('derp', 'APPROVED')

        # A -> B + C (via PR depends)
        # B -> A
        # C -> A
        # X -> A + Y
        # Y -> X
        A.body = "{}\n\nDepends-On: {}\nDepends-On: {}\n".format(
            A.subject, B.url, C.url
        )
        B.body = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.url
        )
        C.body = "{}\n\nDepends-On: {}\n".format(
            C.subject, A.url
        )
        X.body = "{}\n\nDepends-On: {}\nDepends-On: {}\n".format(
            X.subject, Y.url, A.url
        )
        Y.body = "{}\n\nDepends-On: {}\n".format(
            Y.subject, X.url
        )

        self.fake_github.emitEvent(X.addLabel("approved"))
        self.waitUntilSettled()

        self.assertEqual(len(A.comments), 0)
        self.assertEqual(len(B.comments), 0)
        self.assertEqual(len(C.comments), 0)
        self.assertEqual(len(X.comments), 0)
        self.assertEqual(len(Y.comments), 0)
        self.assertFalse(A.is_merged)
        self.assertFalse(B.is_merged)
        self.assertFalse(C.is_merged)
        self.assertFalse(X.is_merged)
        self.assertFalse(Y.is_merged)

    def test_filter_unprotected_branches(self):
        """
        Tests that repo state filtering due to excluding unprotected branches
        doesn't break builds if the items are targeted against different
        branches.
        """
        github = self.fake_github.getGithubClient()
        self.create_branch('gh/project', 'stable/foo')
        github.repo_from_project('gh/project')._set_branch_protection(
            'master', True)
        github.repo_from_project('gh/project')._set_branch_protection(
            'stable/foo', True)
        pevent = self.fake_github.getPushEvent(project='gh/project',
                                               ref='refs/heads/stable/foo')
        self.fake_github.emitEvent(pevent)

        self.create_branch('gh/project1', 'stable/bar')
        github.repo_from_project('gh/project1')._set_branch_protection(
            'master', True)
        github.repo_from_project('gh/project1')._set_branch_protection(
            'stable/bar', True)
        pevent = self.fake_github.getPushEvent(project='gh/project',
                                               ref='refs/heads/stable/bar')
        self.fake_github.emitEvent(pevent)

        # Wait until push events are processed to pick up branch
        # protection settings
        self.waitUntilSettled()

        A = self.fake_github.openFakePullRequest(
            "gh/project", "stable/foo", "A")
        B = self.fake_github.openFakePullRequest(
            "gh/project1", "stable/bar", "B")
        A.addReview('derp', 'APPROVED')
        B.addReview('derp', 'APPROVED')
        B.addLabel("approved")

        # A <-> B
        A.body = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.url
        )
        B.body = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.url
        )

        self.fake_github.emitEvent(A.addLabel("approved"))
        self.waitUntilSettled()

        self.assertEqual(len(A.comments), 2)
        self.assertEqual(len(B.comments), 2)
        self.assertTrue(A.is_merged)
        self.assertTrue(B.is_merged)

    def test_cycle_failed_reporting(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_github.openFakePullRequest("gh/project", "master", "A")
        B = self.fake_github.openFakePullRequest("gh/project1", "master", "B")
        C = self.fake_github.openFakePullRequest("gh/project1", "master", "B")
        A.addReview('derp', 'APPROVED')
        B.addReview('derp', 'APPROVED')
        C.addReview('derp', 'APPROVED')
        B.addLabel("approved")

        # A <-> B
        A.body = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.url
        )
        B.body = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.url
        )

        self.fake_github.emitEvent(A.addLabel("approved"))
        self.waitUntilSettled()
        # Put an indepedente change behind this pair to test that it
        # gets reset.
        self.fake_github.emitEvent(C.addLabel("approved"))
        self.waitUntilSettled()

        # Change draft status of A so it can no longer merge. Note that we
        # don't send an event to test the "github doesn't send an event"
        # case.
        A.draft = True
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(len(A.comments), 2)
        self.assertEqual(len(B.comments), 2)
        self.assertFalse(A.is_merged)
        self.assertFalse(B.is_merged)
        self.assertTrue(C.is_merged)

        self.assertIn("failed to merge",
                      A.comments[-1])
        self.assertTrue(
            re.search("Change https://github.com/gh/project/pull/1 "
                      "can not be merged",
                      A.comments[-1]))
        self.assertFalse(re.search('Change .*? is needed',
                                   A.comments[-1]))

        self.assertIn("failed to merge",
                      B.comments[-1])
        self.assertTrue(
            re.search("Change https://github.com/gh/project/pull/1 "
                      "can not be merged",
                      B.comments[-1]))
        self.assertFalse(re.search('Change .*? is needed',
                                   B.comments[-1]))
        self.assertHistory([
            dict(name="project1-job", result="SUCCESS",
                 changes=f"{B.number},{B.head_sha} {A.number},{A.head_sha}"),
            dict(name="project-vars-job", result="SUCCESS",
                 changes=f"{B.number},{B.head_sha} {A.number},{A.head_sha}"),
            dict(name="project-job", result="SUCCESS",
                 changes=f"{B.number},{B.head_sha} {A.number},{A.head_sha}"),
            dict(name="project1-job", result="SUCCESS",
                 changes=(f"{B.number},{B.head_sha} {A.number},{A.head_sha} "
                          f"{C.number},{C.head_sha}")),
            dict(name="project-vars-job", result="SUCCESS",
                 changes=(f"{B.number},{B.head_sha} {A.number},{A.head_sha} "
                          f"{C.number},{C.head_sha}")),
            dict(name="project1-job", result="SUCCESS",
                 changes=f"{C.number},{C.head_sha}"),
            dict(name="project-vars-job", result="SUCCESS",
                 changes=f"{C.number},{C.head_sha}"),
        ], ordered=False)

    def test_dependency_refresh(self):
        # Test that when two changes are put into a cycle, the
        # dependencies are refreshed and items already in pipelines
        # are updated.
        self.executor_server.hold_jobs_in_build = True

        # This simulates the typical workflow where a developer only
        # knows the PR id of changes one at a time.
        # The first change:
        A = self.fake_github.openFakePullRequest("gh/project", "master", "A")
        self.fake_github.emitEvent(A.getPullRequestOpenedEvent())
        self.waitUntilSettled("Stage 1")

        # Now that it has been uploaded, upload the second change and
        # point it at the first.
        # B -> A
        B = self.fake_github.openFakePullRequest("gh/project", "master", "B")
        B.body = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.url
        )
        self.fake_github.emitEvent(B.getPullRequestOpenedEvent())
        self.waitUntilSettled("Stage 2")

        # Now that the second change is known, update the first change
        # B <-> A
        A.body = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.url
        )

        self.fake_github.emitEvent(A.getPullRequestEditedEvent(A.subject))
        self.waitUntilSettled("Stage 3")

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled("Stage 4")

        self.assertHistory([
            dict(name="project-job", result="ABORTED",
                 changes=f"{A.number},{A.head_sha}"),
            dict(name="project-job", result="ABORTED",
                 changes=f"{A.number},{A.head_sha} {B.number},{B.head_sha}"),
            dict(name="project-job", result="SUCCESS",
                 changes=f"{B.number},{B.head_sha} {A.number},{A.head_sha}",
                 ref="refs/pull/1/head",
                 ),
            dict(name="project-job", result="SUCCESS",
                 changes=f"{B.number},{B.head_sha} {A.number},{A.head_sha}",
                 ref="refs/pull/2/head",
                 ),
        ], ordered=False)


class TestGithubAppCircularDependencies(ZuulGithubAppTestCase):
    config_file = "zuul-gerrit-github-app.conf"
    tenant_config_file = "config/circular-dependencies/main.yaml"
    scheduler_count = 1

    def test_dependency_refresh_checks_api(self):
        # Test that when two changes are put into a cycle, the
        # dependencies are refreshed and items already in pipelines
        # are updated and that the Github check-run is still
        # in-progress.
        self.executor_server.hold_jobs_in_build = True

        # This simulates the typical workflow where a developer only
        # knows the PR id of changes one at a time.
        # The first change:
        A = self.fake_github.openFakePullRequest("gh/project", "master", "A")
        self.fake_github.emitEvent(A.getPullRequestOpenedEvent())
        self.waitUntilSettled()

        # Now that it has been uploaded, upload the second change and
        # point it at the first.
        # B -> A
        B = self.fake_github.openFakePullRequest("gh/project", "master", "B")
        B.body = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.url
        )
        self.fake_github.emitEvent(B.getPullRequestOpenedEvent())
        self.waitUntilSettled()

        # Now that the second change is known, update the first change
        # B <-> A
        A.body = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.url
        )

        self.fake_github.emitEvent(A.getPullRequestEditedEvent(A.subject))
        self.waitUntilSettled()

        # Validate that the Github check-run was cancelled and a new
        # one restarted.
        check_runs = self.fake_github.getCommitChecks("gh/project", A.head_sha)
        self.assertEqual(len(check_runs), 2)
        self.assertEqual(check_runs[0]["status"], "in_progress")
        self.assertEqual(check_runs[1]["status"], "completed")

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name="project-job", result="ABORTED",
                 changes=f"{A.number},{A.head_sha}"),
            dict(name="project-job", result="ABORTED",
                 changes=f"{A.number},{A.head_sha} {B.number},{B.head_sha}"),
            dict(name="project-job", result="SUCCESS",
                 changes=f"{B.number},{B.head_sha} {A.number},{A.head_sha}",
                 ref="refs/pull/1/head"),
            dict(name="project-job", result="SUCCESS",
                 changes=f"{B.number},{B.head_sha} {A.number},{A.head_sha}",
                 ref="refs/pull/2/head"),
        ], ordered=False)
        # Verify that since we are issuing two check runs, they both
        # complete.
        check_runs = self.fake_github.getCommitChecks("gh/project", A.head_sha)
        self.assertEqual(len(check_runs), 2)
        check_run = check_runs[0]
        self.assertEqual(check_run["status"], "completed")
        check_run = check_runs[1]
        self.assertEqual(check_run["status"], "completed")
        self.assertNotEqual(check_runs[0]["id"], check_runs[1]["id"])

        check_runs = self.fake_github.getCommitChecks("gh/project", B.head_sha)
        self.assertEqual(len(check_runs), 2)
        check_run = check_runs[0]
        self.assertEqual(check_run["status"], "completed")
        check_run = check_runs[1]
        self.assertEqual(check_run["status"], "completed")
        self.assertNotEqual(check_runs[0]["id"], check_runs[1]["id"])

    @simple_layout('layouts/dependency_removal_gate.yaml', driver='github')
    def test_dependency_removal_gate(self):
        # Dependency cycles can be updated without uploading new
        # patchsets.  This test exercises such changes.

        self.executor_server.hold_jobs_in_build = True

        A = self.fake_github.openFakePullRequest("gh/project1", "master", "A")
        B = self.fake_github.openFakePullRequest("gh/project2", "master", "B")
        C = self.fake_github.openFakePullRequest("gh/project3", "master", "C")
        D = self.fake_github.openFakePullRequest("gh/project", "master", "D")
        E = self.fake_github.openFakePullRequest("gh/project", "master", "E")

        # Because we call setConfiguration immediately upon removing a
        # change, in order to fully exercise setConfiguration on a
        # queue that has been reloaded from ZK with potentially stale
        # bundle data, we need to remove a change from the bundle
        # twice.  ABC is the main bundle we're interested in.  E is a
        # sacrificial change to force a bundle update when it's
        # removed.

        # E->C, A->E, B->A, C->B
        E.body = "{}\n\nDepends-On: {}\n".format(
            E.subject, C.url
        )
        A.body = "{}\n\nDepends-On: {}\n".format(
            A.subject, E.url
        )
        B.body = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.url
        )
        C.body = "{}\n\nDepends-On: {}\n".format(
            C.subject, B.url
        )

        A.addLabel("approved")
        B.addLabel("approved")
        D.addLabel("approved")
        E.addLabel("approved")

        self.fake_github.emitEvent(C.addLabel("approved"))
        self.waitUntilSettled()
        expected_cycle = {A.number, B.number, C.number, E.number}
        items = self.getAllItems('tenant-one', 'gate')
        self.assertEqual(len(list(items)), 1)
        for item in items:
            cycle = {c.number for c in item.changes}
            self.assertEqual(expected_cycle, cycle)
            # Assert we get the same triggering change every time
            self.assertEqual(json.loads(item.event.ref)['stable_id'], '3')

        # Now we remove the dependency on E.  This re-enqueues ABC and E.

        # A->C, B->A, C->B
        A.body = "{}\n\nDepends-On: {}\n".format(
            A.subject, C.url
        )
        self.fake_github.emitEvent(A.getPullRequestEditedEvent(A.body))
        self.waitUntilSettled()
        items = self.getAllItems('tenant-one', 'gate')
        self.assertEqual(len(list(items)), 2)

        # Now remove all dependencies for the three remaining changes,
        # and also E so that it doesn't get pulled back in as an
        # approved change behind C.
        E.body = E.subject
        A.body = A.subject
        B.body = B.subject
        C.body = C.subject

        self.fake_github.emitEvent(A.getPullRequestEditedEvent(A.body))
        self.waitUntilSettled()
        self.fake_github.emitEvent(B.getPullRequestEditedEvent(B.body))
        self.waitUntilSettled()
        self.fake_github.emitEvent(C.getPullRequestEditedEvent(C.body))
        self.waitUntilSettled()
        items = self.getAllItems('tenant-one', 'gate')
        self.assertEqual(len(list(items)), 3)
        for item in items:
            self.assertEqual(len(item.changes), 1)
            # Assert we get the same triggering change every time
            self.assertEqual(json.loads(item.event.ref)['stable_id'], '3')

        # Remove the first change from the queue by forcing a
        # dependency on D.
        A.body = "{}\n\nDepends-On: {}\n".format(
            A.subject, D.url
        )
        self.fake_github.emitEvent(A.getPullRequestEditedEvent(A.body))
        self.waitUntilSettled()
        items = self.getAllItems('tenant-one', 'gate')
        self.assertEqual(len(list(items)), 2)
        for item in items:
            self.assertEqual(len(item.changes), 1)
            # Assert we get the same triggering change every time
            self.assertEqual(json.loads(item.event.ref)['stable_id'], '3')

        # B and C are still in the queue.  Put them
        # back into a bundle.
        C.body = "{}\n\nDepends-On: {}\n".format(
            C.subject, B.url
        )
        self.fake_github.emitEvent(C.getPullRequestEditedEvent(C.body))
        self.waitUntilSettled()
        B.body = "{}\n\nDepends-On: {}\n".format(
            B.subject, C.url
        )
        self.fake_github.emitEvent(B.getPullRequestEditedEvent(B.body))
        self.waitUntilSettled()
        expected_cycle = {B.number, C.number}
        items = self.getAllItems('tenant-one', 'gate')
        self.assertEqual(len(list(items)), 1)
        for item in items:
            cycle = {c.number for c in item.changes}
            self.assertEqual(expected_cycle, cycle)
            # Assert we get the same triggering change every time
            self.assertEqual(json.loads(item.event.ref)['stable_id'], '3')

        # All done.
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        for build in self.history[:-3]:
            self.assertEqual(build.result, 'ABORTED')
        # Changes B, C in the end
        for build in self.history[-3:]:
            self.assertEqual(build.result, 'SUCCESS')

    @simple_layout('layouts/dependency_removal_gate.yaml', driver='github')
    def test_dependency_addition_gate(self):
        # Dependency cycles can be updated without uploading new
        # patchsets.  This test exercises such changes.

        self.executor_server.hold_jobs_in_build = True
        A = self.fake_github.openFakePullRequest("gh/project1", "master", "A")
        B = self.fake_github.openFakePullRequest("gh/project2", "master", "B")

        B.addLabel("approved")

        self.fake_github.emitEvent(A.addLabel("approved"))
        self.waitUntilSettled()

        items = self.getAllItems('tenant-one', 'gate')
        self.assertEqual(len(list(items)), 1)
        for item in items:
            self.assertEqual(len(item.changes), 1)
            # Assert we get the same triggering change every time
            self.assertEqual(json.loads(item.event.ref)['stable_id'], '1')

        B.body = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.url
        )
        self.fake_github.emitEvent(B.getPullRequestEditedEvent(B.body))
        self.waitUntilSettled()

        A.body = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.url
        )
        self.fake_github.emitEvent(A.getPullRequestEditedEvent(A.body))
        self.waitUntilSettled()
        items = self.getAllItems('tenant-one', 'gate')
        self.assertEqual(len(list(items)), 1)
        expected_cycle = {A.number, B.number}
        for item in items:
            cycle = {c.number for c in item.changes}
            self.assertEqual(expected_cycle, cycle)
            # Assert we get the same triggering change every time
            self.assertEqual(json.loads(item.event.ref)['stable_id'], '1')

        # All done.
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        for build in self.history[:-3]:
            self.assertEqual(build.result, 'ABORTED')
        # Changes A, B in the end
        for build in self.history[-3:]:
            self.assertEqual(build.result, 'SUCCESS')

    def assertQueueCycles(self, pipeline_name, queue_index, bundles):
        queues = self.getAllQueues('tenant-one', pipeline_name)
        queue = queues[queue_index]
        self.assertEqual(len(queue.queue), len(bundles))

        for x, item in enumerate(queue.queue):
            cycle = {c.number for c in item.changes}
            expected_cycle = {c.number for c in bundles[x]}
            self.assertEqual(expected_cycle, cycle)

    @simple_layout('layouts/dependency_removal_check.yaml', driver='github')
    def test_dependency_removal_check(self):
        # Dependency cycles can be updated without uploading new
        # patchsets.  This test exercises such changes.

        self.executor_server.hold_jobs_in_build = True

        A = self.fake_github.openFakePullRequest("gh/project1", "master", "A")
        B = self.fake_github.openFakePullRequest("gh/project2", "master", "B")
        C = self.fake_github.openFakePullRequest("gh/project3", "master", "C")
        E = self.fake_github.openFakePullRequest("gh/project", "master", "E")

        # Because we call setConfiguration immediately upon removing a
        # change, in order to fully exercise setConfiguration on a
        # queue that has been reloaded from ZK with potentially stale
        # bundle data, we need to remove a change from the bundle
        # twice.  ABC is the main bundle we're interested in.  E is a
        # sacrificial change to force a bundle update when it's
        # removed.

        # E->C, A->E, B->A, C->B
        E.body = "{}\n\nDepends-On: {}\n".format(
            E.subject, C.url
        )
        A.body = "{}\n\nDepends-On: {}\n".format(
            A.subject, E.url
        )
        B.body = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.url
        )
        C.body = "{}\n\nDepends-On: {}\n".format(
            C.subject, B.url
        )

        self.fake_github.emitEvent(C.getPullRequestOpenedEvent())
        self.waitUntilSettled()
        abce = [A, B, C, E]
        self.assertEqual(len(self.getAllQueues('tenant-one', 'check')), 1)
        self.assertQueueCycles('check', 0, [abce])
        items = self.getAllItems('tenant-one', 'check')
        for item in items:
            # Assert we get the same triggering change every time
            self.assertEqual(json.loads(item.event.ref)['stable_id'], '3')

        # Now we remove the dependency on E.

        # A->C, B->A, C->B
        A.body = "{}\n\nDepends-On: {}\n".format(
            A.subject, C.url
        )
        self.fake_github.emitEvent(A.getPullRequestEditedEvent(A.body))
        self.waitUntilSettled()

        abc = [A, B, C]
        # ABC<nonlive>, E<live>
        self.assertQueueCycles('check', 0, [abc, [E]])
        items = self.getAllItems('tenant-one', 'check')
        for item in items:
            # Assert we get the same triggering change every time
            self.assertEqual(json.loads(item.event.ref)['stable_id'], '3')

        # Now remove all dependencies for the three remaining changes.
        A.body = A.subject
        B.body = B.subject
        C.body = C.subject

        self.fake_github.emitEvent(A.getPullRequestEditedEvent(A.body))
        self.waitUntilSettled()
        self.fake_github.emitEvent(B.getPullRequestEditedEvent(B.body))
        self.waitUntilSettled()
        self.fake_github.emitEvent(C.getPullRequestEditedEvent(C.body))
        self.waitUntilSettled()

        # A, B, C individually
        self.assertEqual(len(self.getAllQueues('tenant-one', 'check')), 3)
        self.assertQueueCycles('check', 0, [[A], [B]])
        self.assertQueueCycles('check', 1, [[A], [B], [C]])
        self.assertQueueCycles('check', 2, [[A]])
        for item in self.getAllItems('tenant-one', 'check'):
            # Assert we get the same triggering change every time
            self.assertEqual(json.loads(item.event.ref)['stable_id'], '3')

        # Verify that we can put B and C into a bundle.
        C.body = "{}\n\nDepends-On: {}\n".format(
            C.subject, B.url
        )
        self.fake_github.emitEvent(C.getPullRequestEditedEvent(C.body))
        self.waitUntilSettled()
        for item in self.getAllItems('tenant-one', 'check'):
            # Assert we get the same triggering change every time
            self.assertEqual(json.loads(item.event.ref)['stable_id'], '3')
        B.body = "{}\n\nDepends-On: {}\n".format(
            B.subject, C.url
        )
        self.fake_github.emitEvent(B.getPullRequestEditedEvent(B.body))
        self.waitUntilSettled()
        for item in self.getAllItems('tenant-one', 'check'):
            # Assert we get the same triggering change every time
            self.assertEqual(json.loads(item.event.ref)['stable_id'], '3')
        self.assertEqual(len(self.getAllQueues('tenant-one', 'check')), 2)
        bc = [B, C]
        self.assertQueueCycles('check', 0, [[A]])
        self.assertQueueCycles('check', 1, [bc])

        # All done.
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        for build in self.history[:-5]:
            self.assertEqual(build.result, 'ABORTED')
        # Changes A, B, C in the end
        for build in self.history[-5:]:
            self.assertEqual(build.result, 'SUCCESS')

    @simple_layout('layouts/dependency_removal_check.yaml', driver='github')
    def test_dependency_addition_check(self):
        # Dependency cycles can be updated without uploading new
        # patchsets.  This test exercises such changes.

        self.executor_server.hold_jobs_in_build = True
        A = self.fake_github.openFakePullRequest("gh/project1", "master", "A")
        B = self.fake_github.openFakePullRequest("gh/project2", "master", "B")

        self.fake_github.emitEvent(A.getPullRequestOpenedEvent())
        self.waitUntilSettled()

        items = self.getAllItems('tenant-one', 'check')
        self.assertEqual(len(list(items)), 1)
        for item in items:
            self.assertEqual(len(item.changes), 1)
            # Assert we get the same triggering change every time
            self.assertEqual(json.loads(item.event.ref)['stable_id'], '1')

        B.body = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.url
        )
        self.fake_github.emitEvent(B.getPullRequestEditedEvent(B.body))
        self.waitUntilSettled()

        A.body = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.url
        )
        self.fake_github.emitEvent(A.getPullRequestEditedEvent(A.body))
        self.waitUntilSettled()
        items = self.getAllItems('tenant-one', 'check')
        self.assertEqual(len(list(items)), 1)
        for item in items:
            self.assertEqual(len(item.changes), 2)
            # Assert we get the same triggering change every time
            self.assertEqual(json.loads(item.event.ref)['stable_id'], '1')
        self.assertEqual(len(self.getAllQueues('tenant-one', 'check')), 1)
        ab = [A, B]
        self.assertQueueCycles('check', 0, [ab])

        # All done.
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        for build in self.history[:-4]:
            self.assertEqual(build.result, 'ABORTED')
        # A single change in the end
        for build in self.history[-2:]:
            self.assertEqual(build.result, 'SUCCESS')
