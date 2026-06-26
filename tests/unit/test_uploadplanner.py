# Copyright 2025 Acme Gating, LLC
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

from uuid import uuid4
import time

from zuul import model
from zuul.launcher.uploadplanner import UploadPlanner
from zuul.provider import (
    BaseImageCopyJob,
    BaseImageImportJob,
    BaseImageUploadJob,
)
from tests.base import BaseTestCase


def get_uuid(job):
    return job.metadata['zuul_upload_uuid']


class Dummy:
    pass


class FakeImageImportJob(BaseImageImportJob):
    def __init__(self, metadata):
        super().__init__()
        self.metadata = metadata


class FakeImageCopyJob(BaseImageCopyJob):
    def __init__(self, metadata):
        super().__init__()
        self.metadata = metadata


class FakeImageUploadJob(BaseImageUploadJob):
    def __init__(self, metadata):
        super().__init__()
        self.metadata = metadata


class FakeProvider:
    def __init__(self, name):
        self.name = name
        image = Dummy()
        image.name = 'image-name'
        image.canonical_name = 'image-canonical-name'
        image.upload_methods = ['copy', 'import', 'upload']
        self.images = {
            image.name: image,
        }
        self.allow_copy = []

    def getImageImportJob(self, url, provider_image, image_name,
                          image_format, metadata, md5, sha256):
        if f'{self.name}-import' in url:
            return FakeImageImportJob(metadata)
        return None

    def getImageCopyJob(self, source_provider, provider_image,
                        image_name, image_format, metadata, md5, sha256):
        # source image id (source_upload.external_id)
        # source region (source_upload.provider.region)
        # new tags
        # don't copy tags
        if source_provider.name in self.allow_copy:
            return FakeImageCopyJob(metadata)
        return None

    def getImageUploadJob(self, provider_image, image_name,
                          image_format, metadata, md5, sha256):
        return FakeImageUploadJob(metadata)

    def getUploadTags(self, system_id, image, upload_uuid):
        fixed = {
            'zuul_system_id': system_id,
            'zuul_upload_uuid': upload_uuid,
        }
        return fixed


class FakeLauncher:
    def __init__(self):
        self.system = Dummy()
        self.system.system_id = 'system-id'
        self._providers = {
            'p1': FakeProvider('p1'),
            'p2': FakeProvider('p2'),
            'p3': FakeProvider('p3'),
            'p4': FakeProvider('p4'),
        }

    def _getProviderByCanonicalName(self, provider_name):
        return self._providers[provider_name]


class TestUploadPlanner(BaseTestCase):
    def makeArtifact(self, uuid=None, url=None):
        if uuid is None:
            uuid = uuid4().hex
        if url is None:
            url = 'http://example.com/image.raw.zst'
        iba = model.ImageBuildArtifact()
        iba._set(
            uuid=uuid,
            name='image-name',
            canonical_name='image-canonical-name',
            project_canonical_name='project-canonical-name',
            url=url,
            timestamp=time.time(),
        )
        return iba

    def makeUpload(self, iba, config_hash=None, providers=None, uuid=None,
                   state=None, external_id=None):
        if uuid is None:
            uuid = uuid4().hex
        if config_hash is None:
            config_hash = 'config-hash'
        if providers is None:
            providers = ['p1']
        if state is None:
            state = model.ImageUpload.State.PENDING
        upload = model.ImageUpload()
        upload._set(
            uuid=uuid,
            artifact_uuid=iba.uuid,
            endpoint_name='endpoint-name',
            providers=providers,
            canonical_name='image-canonical-name',
            config_hash=config_hash,
            timestamp=time.time(),
            _state=model.ImageUpload.State.PENDING,
            state_time=time.time(),
            external_id=external_id,
        )
        return upload

    def printJobs(self, planner):
        for x in planner.import_jobs:
            self.log.debug("Import job: %s", x[1].metadata)
        for x in planner.upload_jobs:
            self.log.debug("Upload job: %s", x[1].metadata)
        for x in planner.copy_jobs:
            self.log.debug("Copy job: %s", x[1].metadata)

    def test_one_upload(self):
        # There's only one upload, we should have an upload job for it
        iba = self.makeArtifact()
        upload = self.makeUpload(iba)
        launcher = FakeLauncher()
        pending_uploads = [upload]
        all_uploads = [upload]
        planner = UploadPlanner(launcher, iba, pending_uploads, all_uploads)
        planner.plan()

        self.printJobs(planner)
        self.assertEqual([], planner.import_jobs)
        self.assertEqual([upload.uuid],
                         [get_uuid(x[1]) for x in planner.upload_jobs])
        self.assertEqual([], planner.copy_jobs)

    def test_one_import(self):
        # There's only one upload, we should have an import job for it
        iba = self.makeArtifact(url='p1-import')
        upload = self.makeUpload(iba)
        launcher = FakeLauncher()
        pending_uploads = [upload]
        all_uploads = [upload]
        planner = UploadPlanner(launcher, iba, pending_uploads, all_uploads)
        planner.plan()

        self.printJobs(planner)
        self.assertEqual([upload.uuid],
                         [get_uuid(x[1]) for x in planner.import_jobs])
        self.assertEqual([], planner.upload_jobs)
        self.assertEqual([], planner.copy_jobs)

    def test_import_copy(self):
        # We import, then copy from that
        iba = self.makeArtifact(url='p1-import')
        upload_import = self.makeUpload(iba, providers=['p1'])
        upload_copy = self.makeUpload(iba, providers=['p2'])
        launcher = FakeLauncher()
        launcher._providers['p2'].allow_copy.append('p1')
        pending_uploads = [upload_import, upload_copy]
        all_uploads = [upload_import, upload_copy]
        planner = UploadPlanner(launcher, iba, pending_uploads, all_uploads)
        planner.plan()

        self.printJobs(planner)
        self.assertEqual([upload_import.uuid],
                         [get_uuid(x[1]) for x in planner.import_jobs])
        self.assertEqual([], planner.upload_jobs)
        self.assertEqual([], planner.copy_jobs)
        self.assertEqual(
            [upload_copy.uuid],
            [get_uuid(x[1]) for x in planner.import_jobs[0][1].dependents])

    def test_upload_copy(self):
        # We upload, then copy from that
        iba = self.makeArtifact()
        upload_upload = self.makeUpload(iba, providers=['p1'])
        upload_copy = self.makeUpload(iba, providers=['p2'])
        launcher = FakeLauncher()
        launcher._providers['p2'].allow_copy.append('p1')
        pending_uploads = [upload_upload, upload_copy]
        all_uploads = [upload_upload, upload_copy]
        planner = UploadPlanner(launcher, iba, pending_uploads, all_uploads)
        planner.plan()

        self.printJobs(planner)
        self.assertEqual([], planner.import_jobs)
        self.assertEqual([upload_upload.uuid],
                         [get_uuid(x[1]) for x in planner.upload_jobs])
        self.assertEqual([], planner.copy_jobs)
        self.assertEqual(
            [upload_copy.uuid],
            [get_uuid(x[1]) for x in planner.upload_jobs[0][1].dependents])

    def test_multi_import_copy(self):
        # Two imports with copies
        iba = self.makeArtifact(url='p1-import p2-import')
        upload_import1 = self.makeUpload(iba, providers=['p1'])
        upload_import2 = self.makeUpload(iba, providers=['p2'])
        upload_copy1 = self.makeUpload(iba, providers=['p3'])
        upload_copy2 = self.makeUpload(iba, providers=['p4'])
        launcher = FakeLauncher()
        pending_uploads = [upload_import1, upload_copy1,
                           upload_import2, upload_copy2]

        # (from, to)
        launcher._providers['p4'].allow_copy.append('p1')
        launcher._providers['p3'].allow_copy.append('p2')
        all_uploads = pending_uploads[:]
        planner = UploadPlanner(launcher, iba, pending_uploads, all_uploads)
        planner.plan()

        self.printJobs(planner)
        self.assertEqual([upload_import1.uuid, upload_import2.uuid],
                         [get_uuid(x[1]) for x in planner.import_jobs])
        self.assertEqual([], planner.upload_jobs)
        self.assertEqual([], planner.copy_jobs)
        self.assertEqual(
            [upload_copy2.uuid],
            [get_uuid(x[1]) for x in planner.import_jobs[0][1].dependents])
        self.assertEqual(
            [upload_copy1.uuid],
            [get_uuid(x[1]) for x in planner.import_jobs[1][1].dependents])

    def test_multi_upload_copy(self):
        # Two uploads with copies
        iba = self.makeArtifact(url='p1-upload')
        upload_upload1 = self.makeUpload(iba, providers=['p1'])
        upload_upload2 = self.makeUpload(iba, providers=['p2'])
        upload_copy1 = self.makeUpload(iba, providers=['p3'])
        upload_copy2 = self.makeUpload(iba, providers=['p4'])
        launcher = FakeLauncher()
        pending_uploads = [upload_upload1, upload_copy1,
                           upload_upload2, upload_copy2]

        # (from, to)
        launcher._providers['p4'].allow_copy.append('p1')
        launcher._providers['p3'].allow_copy.append('p2')
        all_uploads = pending_uploads[:]
        planner = UploadPlanner(launcher, iba, pending_uploads, all_uploads)
        planner.plan()

        self.printJobs(planner)
        self.assertEqual([upload_upload1.uuid, upload_upload2.uuid],
                         [get_uuid(x[1]) for x in planner.upload_jobs])
        self.assertEqual([], planner.import_jobs)
        self.assertEqual([], planner.copy_jobs)
        self.assertEqual(
            [upload_copy2.uuid],
            [get_uuid(x[1]) for x in planner.upload_jobs[0][1].dependents])
        self.assertEqual(
            [upload_copy1.uuid],
            [get_uuid(x[1]) for x in planner.upload_jobs[1][1].dependents])

    def test_finished_import_copy(self):
        # Copy from a finished import
        iba = self.makeArtifact(url='p1-import')
        upload_import = self.makeUpload(iba, providers=['p1'],
                                        state=model.ImageUpload.State.READY,
                                        external_id='test_external_id')
        upload_copy = self.makeUpload(iba, providers=['p2'])
        launcher = FakeLauncher()
        launcher._providers['p2'].allow_copy.append('p1')
        pending_uploads = [upload_copy]
        all_uploads = [upload_import, upload_copy]
        planner = UploadPlanner(launcher, iba, pending_uploads, all_uploads)
        planner.plan()

        self.printJobs(planner)
        self.assertEqual([],
                         [get_uuid(x[1]) for x in planner.import_jobs])
        self.assertEqual([], planner.upload_jobs)
        self.assertEqual([upload_copy.uuid],
                         [get_uuid(x[1]) for x in planner.copy_jobs])
        for j in planner.copy_jobs:
            self.assertEqual([], j[1].dependents)

    def test_finished_upload_copy(self):
        # Copy from a finished upload
        iba = self.makeArtifact()
        upload_upload = self.makeUpload(iba, providers=['p1'],
                                        state=model.ImageUpload.State.READY,
                                        external_id='test_external_id')
        upload_copy = self.makeUpload(iba, providers=['p2'])
        launcher = FakeLauncher()
        launcher._providers['p2'].allow_copy.append('p1')
        pending_uploads = [upload_copy]
        all_uploads = [upload_upload, upload_copy]
        planner = UploadPlanner(launcher, iba, pending_uploads, all_uploads)
        planner.plan()

        self.printJobs(planner)
        self.assertEqual([],
                         [get_uuid(x[1]) for x in planner.upload_jobs])
        self.assertEqual([], planner.import_jobs)
        self.assertEqual([upload_copy.uuid],
                         [get_uuid(x[1]) for x in planner.copy_jobs])
        for j in planner.copy_jobs:
            self.assertEqual([], j[1].dependents)
