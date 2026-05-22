# Copyright 2024 Acme Gating, LLC
# Copyright 2024 BMW Group
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

import itertools
import logging
import math
import os
import queue
import re
import textwrap
import threading
import time
import uuid
from collections import defaultdict, Counter
from unittest import mock

from zuul import exceptions
from zuul import model
import zuul.driver.aws.awsendpoint
from zuul.launcher.client import LauncherClient
import zuul.launcher.server
from zuul.zk.event_queues import PipelineResultEventQueue
from zuul.zk.launcher import RequestCache
from zuul.zk.locks import pipeline_lock

import cachetools
import fixtures
import responses
import testtools
import yaml
from kazoo.exceptions import NoNodeError
from moto import mock_aws
import moto.ec2.responses.instances
import boto3

from tests.base import (
    ResponsesFixture,
    AnsibleZuulTestCase,
    ZuulTestCase,
    driver_config,
    iterate_timeout,
    okay_tracebacks,
    return_data,
    simple_layout,
)
from tests.fake_nodescan import NodescanFixture


class ImageMocksFixture(fixtures.Fixture):
    raw_body = 'test raw image'
    raw_sha256 = ('d043e8080c82dbfeca3199a24d5f019'
                  '3e66755b5ba62d6b60107a248996a6795')
    raw_md5sum = '78d2d3ff2463bc75c7cc1d38b8df6a6b'
    zst_body = b'(\xb5/\xfd\x04Xq\x00\x00test raw image\xc4\xcf\x97b'
    qcow2_body = 'test qcow2 image' * 4096
    qcow2_sha256 = ('6d30f48ee68786c12c7a6915dad1a7bd8'
                    '24b85a13c629583465de97bfb3f5b51')
    qcow2_md5sum = 'a2e23f2a097ac05d85dfebf44841869c'
    vhd_body = 'test vhd image' * 4096
    vhd_sha256 = ('1ba1903d07465751d996e5f995d13411df2'
                  '6d346eb289dcb3d430e05eeb237a8')
    vhd_md5sum = '81a6ad087b65300dcc61c2e543b6ba28'

    def __init__(self, responses_fixture):
        super().__init__()
        self.requests_mock = responses_fixture.requests_mock
        self.requests_mock.add(
            responses.GET,
            'http://example.com/image.raw',
            body=self.raw_body)
        self.requests_mock.add(
            responses.GET,
            'http://example.com/image.raw.zst',
            body=self.zst_body)
        self.requests_mock.add(
            responses.GET,
            'http://example.com/image.qcow2',
            body=self.qcow2_body)
        self.requests_mock.add(
            responses.GET,
            'http://example.com/image.vhd',
            body=self.vhd_body)
        self.requests_mock.add(
            responses.HEAD,
            'http://example.com/image.raw',
            headers={'content-length': str(len(self.raw_body))})
        self.requests_mock.add(
            responses.HEAD,
            'http://example.com/image.raw.zst',
            headers={'content-length': str(len(self.zst_body))})
        self.requests_mock.add(
            responses.HEAD,
            'http://example.com/image.qcow2',
            headers={'content-length': str(len(self.qcow2_body))})
        self.requests_mock.add(
            responses.HEAD,
            'http://example.com/image.vhd',
            headers={'content-length': str(len(self.vhd_body))})
        # The next three are for the signed_url test
        # Partial response
        self.requests_mock.add(
            responses.GET,
            'http://example.com/getonly.raw',
            match=[responses.matchers.header_matcher({"Range": "bytes=0-0"})],
            status=206,
            headers={'content-length': '1',
                     'content-range': f'bytes 0-0/{len(self.raw_body)}'},
        )
        # The full response
        self.requests_mock.add(
            responses.GET,
            'http://example.com/getonly.raw',
            headers={'content-length': str(len(self.raw_body))},
            body=self.raw_body)
        # Head doesn't work
        self.requests_mock.add(
            responses.HEAD,
            'http://example.com/getonly.raw',
            status=403)


class LauncherBaseTestCase(ZuulTestCase):
    config_file = 'zuul-connections-nodepool.conf'
    mock_aws = mock_aws()
    debian_return_data = {
        'zuul': {
            'artifacts': [
                {
                    'name': 'raw image',
                    'url': 'http://example.com/image.raw.zst',
                    'metadata': {
                        'type': 'zuul_image',
                        'image_name': 'debian-local',
                        'format': 'raw',
                        'sha256': ImageMocksFixture.raw_sha256,
                        'md5sum': ImageMocksFixture.raw_md5sum,
                    }
                }, {
                    'name': 'qcow2 image',
                    'url': 'http://example.com/image.qcow2',
                    'metadata': {
                        'type': 'zuul_image',
                        'image_name': 'debian-local',
                        'format': 'qcow2',
                        'sha256': ImageMocksFixture.qcow2_sha256,
                        'md5sum': ImageMocksFixture.qcow2_md5sum,
                    }
                },
            ]
        }
    }
    ubuntu_return_data = {
        'zuul': {
            'artifacts': [
                {
                    'name': 'raw image',
                    'url': 'http://example.com/image.raw.zst',
                    'metadata': {
                        'type': 'zuul_image',
                        'image_name': 'ubuntu-local',
                        'format': 'raw',
                        'sha256': ImageMocksFixture.raw_sha256,
                        'md5sum': ImageMocksFixture.raw_md5sum,
                    }
                }, {
                    'name': 'qcow2 image',
                    'url': 'http://example.com/image.qcow2',
                    'metadata': {
                        'type': 'zuul_image',
                        'image_name': 'ubuntu-local',
                        'format': 'qcow2',
                        'sha256': ImageMocksFixture.qcow2_sha256,
                        'md5sum': ImageMocksFixture.qcow2_md5sum,
                    }
                },
            ]
        }
    }

    def setUp(self):
        self.initTestConfig()

        # Patch moto describe_instances as it isn't terribly threadsafe
        orig_describe_instances = \
            moto.ec2.responses.instances.InstanceResponse.describe_instances

        def describe_instances(self):
            for x in range(10):
                try:
                    return orig_describe_instances(self)
                except RuntimeError:
                    # describe_instances can fail if the reservations dict
                    # changes while it renders its template. Ignore the
                    # error and retry.
                    pass
        self.patch(moto.ec2.responses.instances.InstanceResponse,
                   'describe_instances',
                   describe_instances)

        self.mock_aws.start()
        # Must start responses after mock_aws
        responses_fixture = self.useFixture(ResponsesFixture())
        self.useFixture(ImageMocksFixture(responses_fixture))
        self.s3 = boto3.resource('s3', region_name='us-east-1')
        self.s3.create_bucket(Bucket='zuul')
        self.addCleanup(self.mock_aws.stop)

        quotas = {}
        quotas.update(self.test_config.driver.test_launcher.get(
            'quotas', {}))

        def getEndpointLimits(self):
            return model.QuotaInformation(default=math.inf, **quotas)
        self.patch(zuul.driver.aws.awsprovider.AwsProvider,
                   'getEndpointLimits',
                   getEndpointLimits)

        def refreshQuotaLimits(self, *args, **kw):
            return False
        self.patch(zuul.driver.aws.awsendpoint.AwsProviderEndpoint,
                   'refreshQuotaLimits',
                   refreshQuotaLimits)

        self.lateSetUp()
        super().setUp()

    def lateSetUp(self):
        pass

    def getNodes(self, request):
        nodes = []
        for node in self.launcher.api.nodes_cache.getItems():
            if node.request_id == request.uuid:
                nodes.append(node)
        return nodes

    def _nodes_by_label(self):
        nodes = self.launcher.api.nodes_cache.getItems()
        nodes_by_label = defaultdict(list)
        for node in nodes:
            nodes_by_label[node.label].append(node)
        return nodes_by_label


class TestLauncher(LauncherBaseTestCase):

    def _waitForArtifacts(self, image_name, count):
        for _ in iterate_timeout(30, "artifacts to settle"):
            artifacts = self.launcher.image_build_registry.\
                getArtifactsForImage(image_name)
            if len(artifacts) == count:
                return artifacts

    def _waitForUploads(self, image_cname, count=None):
        for _ in iterate_timeout(60, "upload to complete"):
            uploads = self.launcher.image_upload_registry.getUploadsForImage(
                image_cname)
            pending = [u for u in uploads if u.external_id is None]
            if not pending:
                if count is None or count == len(uploads):
                    return uploads

    def _waitForNoChildren(self, path):
        for _ in iterate_timeout(10, f"empty path {path}"):
            znodes = self.zk_client.client.get_children('/zuul/nodeset/locks')
            if not len(znodes):
                return

    @simple_layout('layouts/nodepool-missing-connection.yaml',
                   enable_nodepool=True)
    def test_launcher_missing_connection(self):
        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        errors = tenant.layout.loading_errors
        self.assertEqual(len(errors), 1)

        idx = 0
        self.assertEqual(errors[idx].severity, model.SEVERITY_ERROR)
        self.assertEqual(errors[idx].name, 'Unknown Connection')
        self.assertIn('provider stanza', errors[idx].error)

    def _test_launcher_image_expire(self, count):
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local', 'ubuntu-local'],
        )
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))

        for _ in iterate_timeout(
                30, "scheduler and launcher to have the same layout"):
            if (self.scheds.first.sched.local_layout_state.get("tenant-one") ==
                self.launcher.local_layout_state.get("tenant-one")):
                break

        # The build should not run again because the image is no
        # longer missing
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)
        for name in [
                'review.example.com%2Forg%2Fcommon-config/debian-local',
                'review.example.com%2Forg%2Fcommon-config/ubuntu-local',
        ]:
            artifacts = self._waitForArtifacts(name, 1)
            self.assertEqual('raw', artifacts[0].format)
            self.assertTrue(artifacts[0].validated)
            uploads = self.launcher.image_upload_registry.getUploadsForImage(
                name)
            self.assertEqual(1, len(uploads))
            self.assertEqual(artifacts[0].uuid, uploads[0].artifact_uuid)
            self.assertEqual("test_external_id", uploads[0].external_id)
            self.assertTrue(uploads[0].validated)

        image_cname = 'review.example.com%2Forg%2Fcommon-config/ubuntu-local'
        artifacts1 = self._waitForArtifacts(image_cname, 1)
        artifacts1_uuids = set([x.uuid for x in artifacts1])

        # Run another build event manually
        driver = self.launcher.connections.drivers['zuul']
        event = driver.getImageBuildEvent(
            ['ubuntu-local'], 'review.example.com',
            'org/common-config', 'master')
        self.launcher.trigger_events['tenant-one'].put(
            event.trigger_name, event)
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)
        artifacts2 = self._waitForArtifacts(image_cname, 2)
        artifacts2_uuids = set([x.uuid for x in artifacts2])
        self.assertTrue(artifacts1_uuids < artifacts2_uuids)

        # Run another build event manually
        driver = self.launcher.connections.drivers['zuul']
        event = driver.getImageBuildEvent(
            ['ubuntu-local'], 'review.example.com',
            'org/common-config', 'master')
        self.launcher.trigger_events['tenant-one'].put(
            event.trigger_name, event)
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)
        artifacts3 = self._waitForArtifacts(image_cname, count)
        artifacts3_uuids = set([x.uuid for x in artifacts3])
        if count == 2:
            self.assertFalse(artifacts3_uuids.isdisjoint(artifacts2_uuids))
            self.assertTrue(artifacts3_uuids.isdisjoint(artifacts1_uuids))
        else:
            self.assertTrue(artifacts2_uuids < artifacts3_uuids)

    @simple_layout('layouts/nodepool-image.yaml', enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @return_data(
        'build-ubuntu-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.ubuntu_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="test_external_id")
    def test_launcher_image_expire(self, mock_image_upload_run):
        self._test_launcher_image_expire(count=2)

    @simple_layout('layouts/nodepool-image-retain-count.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @return_data(
        'build-ubuntu-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.ubuntu_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="test_external_id")
    def test_launcher_image_expire_retain_count(self, mock_image_upload_run):
        self._test_launcher_image_expire(count=3)

    @simple_layout('layouts/nodepool-image-no-validate.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="test_external_id")
    def test_launcher_image_no_validation(self, mock_uploadimage):
        # Test a two-stage image-build where we don't actually run the
        # validate stage (so all artifacts should be un-validated).
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
        ])
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))

        for _ in iterate_timeout(
                30, "scheduler and launcher to have the same layout"):
            if (self.scheds.first.sched.local_layout_state.get("tenant-one") ==
                self.launcher.local_layout_state.get("tenant-one")):
                break

        # The build should not run again because the image is no
        # longer missing
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
        ])
        name = 'review.example.com%2Forg%2Fcommon-config/debian-local'
        artifacts = self._waitForArtifacts(name, 1)
        self.assertEqual('raw', artifacts[0].format)
        self.assertFalse(artifacts[0].validated)
        uploads = self.launcher.image_upload_registry.getUploadsForImage(
            name)
        self.assertEqual(1, len(uploads))
        self.assertEqual(artifacts[0].uuid, uploads[0].artifact_uuid)
        self.assertEqual("test_external_id", uploads[0].external_id)
        self.assertFalse(uploads[0].validated)

    @simple_layout('layouts/nodepool-image-no-validate.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="test_external_id")
    def test_launcher_image_expire_no_validation(self, mock_uploadimage):
        # Test a two-stage image-build where we don't actually run the
        # validate stage (so all artifacts should be un-validated).
        # Test that we expire the unvalidated images.
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled()
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-debian-local-image', result='SUCCESS'),
        ])

        name = 'review.example.com%2Forg%2Fcommon-config/debian-local'
        artifacts = self._waitForArtifacts(name, 2)
        self.assertEqual('raw', artifacts[0].format)
        self.assertFalse(artifacts[0].validated)
        self.assertFalse(artifacts[1].validated)
        uploads = self.launcher.image_upload_registry.getUploadsForImage(
            name)
        self.assertEqual(2, len(uploads))
        self.assertFalse(uploads[0].validated)
        self.assertFalse(uploads[1].validated)
        upload_ids_1 = [u.uuid for u in uploads]

        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-debian-local-image', result='SUCCESS'),
        ])
        # We should still only have two, because one of the old ones
        # should be deleted.
        artifacts = self._waitForArtifacts(name, 2)
        self.assertEqual('raw', artifacts[0].format)
        self.assertFalse(artifacts[0].validated)
        self.assertFalse(artifacts[1].validated)
        uploads = self.launcher.image_upload_registry.getUploadsForImage(
            name)
        self.assertEqual(2, len(uploads))
        self.assertFalse(uploads[0].validated)
        self.assertFalse(uploads[1].validated)
        upload_ids_2 = [u.uuid for u in uploads]
        self.assertNotEqual(upload_ids_1, upload_ids_2)

    @simple_layout('layouts/nodepool-image-validate.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run')
    def test_launcher_image_validation(self, mock_image_upload_run):
        # Test a two-stage image-build where we do run the validate
        # stage.
        self.executor_server.hold_jobs_in_build = True
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled()

        # Use distinct AMI IDs for each expected upload and
        # fail other uploads (there shouldn't be any).
        upload_ids = {"ami-1e749f67", "ami-785db401"}
        mock_image_upload_run.side_effect = itertools.chain.from_iterable((
            upload_ids, itertools.repeat(RuntimeError)
        ))

        self.executor_server.release('build-debian-local-image')
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
        ])

        nodes = self.launcher.api.getProviderNodes()
        self.assertEqual(len(nodes), 2)
        # We should have one node for each image that we uploaded
        external_ids = set(n.create_state['image_external_id'] for n in nodes)
        self.assertEqual(upload_ids, external_ids)
        # Each node should be for a different provider
        self.assertEqual(2, len(set(n.provider for n in nodes)))

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='validate-debian-local-image', result='SUCCESS'),
            dict(name='validate-debian-local-image', result='SUCCESS'),
        ])

        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        for _ in iterate_timeout(
                30, "scheduler and launcher to have the same layout"):
            if (self.scheds.first.sched.local_layout_state.get("tenant-one") ==
                self.launcher.local_layout_state.get("tenant-one")):
                break

        # The build should not run again because the image is no
        # longer missing
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='validate-debian-local-image', result='SUCCESS'),
            dict(name='validate-debian-local-image', result='SUCCESS'),
        ])

        name = 'review.example.com%2Forg%2Fcommon-config/debian-local'
        artifact = self._waitForArtifacts(name, 1)[0]
        self.assertEqual('raw', artifact.format)
        self.assertFalse(artifact.validated)
        uploads = self.launcher.image_upload_registry.getUploadsForImage(
            name)
        self.assertEqual(2, len(uploads))
        for upload in uploads:
            self.assertEqual(artifact.uuid, upload.artifact_uuid)
            self.assertIn(upload.external_id, upload_ids)
            self.assertTrue(upload.validated)
            self.assertIsNotNone(upload.build_uuid)

    @simple_layout('layouts/nodepool-image-validate.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="ami-785db401")
    def test_launcher_image_validation_failure(self, mock_image_upload_run):
        # Test a two-stage image-build where the validate stage fails.
        self.executor_server.hold_jobs_in_build = True
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled()

        self.executor_server.release('build-debian-local-image')
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
        ])

        nodes = self.launcher.api.getProviderNodes()
        self.assertEqual(len(nodes), 2)
        for node in nodes:
            self.assertEqual(node.create_state['image_external_id'],
                             mock_image_upload_run.return_value)

        self.assertEqual(len(self.builds), 2)
        for build in self.builds:
            build.should_fail = True

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='validate-debian-local-image', result='FAILURE'),
            dict(name='validate-debian-local-image', result='FAILURE'),
        ])

        name = 'review.example.com%2Forg%2Fcommon-config/debian-local'
        artifact = self._waitForArtifacts(name, 1)[0]
        self.assertEqual('raw', artifact.format)
        self.assertFalse(artifact.validated)
        uploads = self.launcher.image_upload_registry.getUploadsForImage(
            name)
        self.assertEqual(2, len(uploads))
        for upload in uploads:
            self.assertEqual(artifact.uuid, upload.artifact_uuid)
            self.assertFalse(upload.validated)
            self.assertIsNotNone(upload.build_uuid)

    @simple_layout('layouts/nodepool-image.yaml', enable_nodepool=True)
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="test_external_id")
    def test_launcher_crashed_upload(self, mock_image_upload_run):
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled()
        provider = self.launcher._getProvider(
            'tenant-one', 'aws-us-east-1-main')
        endpoint = provider.getEndpoint()
        image = list(provider.images.values())[1]
        self.assertEqual('debian-local', image.name)
        # create an IBA and an upload
        with self.createZKContext(None) as ctx:
            # This starts with an unknown state, then
            # createImageUploads will set it to ready.
            iba = model.ImageBuildArtifact.new(
                ctx,
                uuid='iba-uuid',
                name=image.name,
                canonical_name=image.canonical_name,
                project_canonical_name=image.project_canonical_name,
                url='http://example.com/image.raw.zst',
                md5sum=ImageMocksFixture.raw_md5sum,
                sha256=ImageMocksFixture.raw_sha256,
                timestamp=time.time(),
            )
            with iba.locked(ctx):
                model.ImageUpload.new(
                    ctx,
                    uuid='upload-uuid',
                    artifact_uuid='iba-uuid',
                    endpoint_name=endpoint.canonical_name,
                    providers=[provider.canonical_name],
                    canonical_name=image.canonical_name,
                    config_hash=image.config_hash,
                    timestamp=time.time(),
                    _state=model.ImageUpload.State.UPLOADING,
                    state_time=time.time(),
                )
                with iba.activeContext(ctx):
                    iba.state = iba.State.READY
        self.waitUntilSettled()
        pending_uploads = [
            u for u in self.launcher.image_upload_registry.getItems()
            if u.state == u.State.PENDING]
        self.assertEqual(0, len(pending_uploads))

    @simple_layout('layouts/nodepool-image.yaml', enable_nodepool=True)
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                side_effect="test_external_id")
    def test_launcher_no_multiple_uploads(self, mock_image_upload_run):
        # Make sure that we don't enqueue multiple upload jobs for the
        # same pending uploads
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled()
        provider = self.launcher._getProvider(
            'tenant-one', 'aws-us-east-1-main')
        endpoint = provider.getEndpoint()
        image = list(provider.images.values())[1]
        self.assertEqual('debian-local', image.name)

        upload_event = threading.Event()
        upload_counter = 0
        orig_upload_run = zuul.launcher.server.UploadJob.run

        def fake_upload_run(*args, **kw):
            nonlocal upload_counter
            upload_counter += 1
            upload_event.wait()
            return orig_upload_run(*args, **kw)

        self.useFixture(fixtures.MonkeyPatch(
            'zuul.launcher.server.UploadJob.run',
            fake_upload_run))

        # create an IBA and an upload
        with self.createZKContext(None) as ctx:
            # This starts with an unknown state, then
            # createImageUploads will set it to ready.
            iba = model.ImageBuildArtifact.new(
                ctx,
                uuid='iba-uuid',
                name=image.name,
                canonical_name=image.canonical_name,
                project_canonical_name=image.project_canonical_name,
                url='http://example.com/image.raw.zst',
                md5sum=ImageMocksFixture.raw_md5sum,
                sha256=ImageMocksFixture.raw_sha256,
                timestamp=time.time(),
            )
            with iba.locked(ctx):
                model.ImageUpload.new(
                    ctx,
                    uuid='upload-uuid',
                    artifact_uuid='iba-uuid',
                    endpoint_name=endpoint.canonical_name,
                    providers=[provider.canonical_name],
                    canonical_name=image.canonical_name,
                    timestamp=time.time(),
                    _state=model.ImageUpload.State.UPLOADING,
                    state_time=time.time(),
                )
                with iba.activeContext(ctx):
                    iba.state = iba.State.READY
        with self.launcher._test_lock:
            self.launcher.checkMissingUploads()
            self.launcher.checkMissingUploads()
        upload_event.set()
        self.waitUntilSettled()
        self.assertEqual(1, upload_counter)

        pending_uploads = [
            u for u in self.launcher.image_upload_registry.getItems()
            if u.state == u.State.PENDING]
        self.assertEqual(0, len(pending_uploads))

    getonly_return_data = {
        'zuul': {
            'artifacts': [
                {
                    'name': 'raw image',
                    'url': 'http://example.com/getonly.raw?Dummy-Token=foo',
                    'metadata': {
                        'type': 'zuul_image',
                        'image_name': 'debian-local',
                        'format': 'raw',
                        'sha256': ImageMocksFixture.raw_sha256,
                        'md5sum': ImageMocksFixture.raw_md5sum,
                    }
                },
            ]
        }
    }

    @simple_layout('layouts/nodepool-image.yaml', enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        getonly_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="test_external_id")
    def test_launcher_image_signed_url(self, mock_image_upload_run):
        # If the image is uploaded using a signed url, it will not
        # permit a HEAD request; this tests the GET range fallback.
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local', 'ubuntu-local'],
        )
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)
        name = 'review.example.com%2Forg%2Fcommon-config/debian-local'
        artifacts = self._waitForArtifacts(name, 1)
        self.assertEqual('raw', artifacts[0].format)
        self.assertTrue(artifacts[0].validated)
        uploads = self.launcher.image_upload_registry.getUploadsForImage(
            name)
        self.assertEqual(1, len(uploads))
        self.assertEqual(artifacts[0].uuid, uploads[0].artifact_uuid)
        self.assertEqual("test_external_id", uploads[0].external_id)
        self.assertTrue(uploads[0].validated)

    @simple_layout('layouts/nodepool-image.yaml', enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        getonly_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="test_external_id")
    def test_launcher_image_cleanup(self, mock_image_upload_run):
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local', 'ubuntu-local'],
        )
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)

        # Clear out Zuul config to remove the provider, which
        # has the same effect as a provider config error.
        files = {'zuul.yaml': ""}
        self.addCommitToRepo(
            'org/common-config', 'Change label config', files)

        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        def _assert_image_state():
            name = 'review.example.com%2Forg%2Fcommon-config/debian-local'
            artifacts = self._waitForArtifacts(name, 1)
            self.assertEqual('raw', artifacts[0].format)
            self.assertTrue(artifacts[0].validated)

            uploads = self.launcher.image_upload_registry.getUploadsForImage(
                name)
            self.assertEqual(1, len(uploads))
            self.assertEqual(artifacts[0].uuid, uploads[0].artifact_uuid)
            self.assertEqual("test_external_id", uploads[0].external_id)
            self.assertTrue(uploads[0].validated)

        self.launcher.checkOldImages()
        _assert_image_state()

        self.commitConfigUpdate(
            "org/common-config", 'layouts/nodepool-image.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        self.launcher.checkOldImages()
        _assert_image_state()

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_jobs_executed(self):
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(nodes[0].host_keys, [])
        provider = self.launcher._getProvider(
            'tenant-one', 'aws-us-east-1-main')
        used = provider.getEndpoint().quota_cache.getUnmanagedUsage()
        self.assertEqual(0, used.getResources().get('instances', 0))
        self.launcher._runStats()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(self.getJobFromHistory('check-job').result,
                         'SUCCESS')
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2)
        self.assertEqual(self.getJobFromHistory('check-job').node,
                         'debian-normal')
        pname = 'review_example_com%2Forg%2Fcommon-config_aws-us-east-1-main'
        self.assertReportedStat(
            f'zuul.provider.{pname}.nodes.state.in-use',
            kind='g')
        self.assertReportedStat(
            f'zuul.provider.{pname}.label.debian-normal.nodes.state.in-use',
            kind='g')
        self.assertReportedStat(
            'zuul.nodes.state.in-use',
            kind='g')
        for _ in iterate_timeout(60, "nodes to be deleted"):
            if len(self.launcher.api.nodes_cache.getItems()) == 0:
                break
        self._waitForNoChildren('/zuul/nodeset/locks')
        self._waitForNoChildren('/zuul/nodeset/requests')
        self._waitForNoChildren('/zuul/nodes/locks')
        self._waitForNoChildren('/zuul/nodes/nodes')

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_zuul_node_data(self):
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        build = self.getBuildByName('check-job')
        inv_path = os.path.join(build.jobdir.root, 'ansible', 'inventory.yaml')
        with open(inv_path, 'r') as f:
            inventory = yaml.safe_load(f)
        zuul_node_vars = inventory['all']['hosts']['controller']['zuul_node']
        instance_id = zuul_node_vars['node_properties']['instance_id']
        self.assertTrue(instance_id.startswith('i-'))
        ebs_volume_id = zuul_node_vars['node_properties']['ebs_volume_id']
        self.assertTrue(ebs_volume_id.startswith('vol-'))
        self.assertFalse(zuul_node_vars['reuse'])

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

    @simple_layout('layouts/nodepool-image-attrs.yaml', enable_nodepool=True)
    def test_node_image_attributes(self):
        # Test that we supply node attributes from the image
        # configuration (assuming the cloud does not override these).
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        nodes = self.launcher.api.nodes_cache.getItems()
        node = nodes[0]
        self.assertEqual([], node.host_keys)
        self.assertEqual(8080, node.connection_port)
        self.assertEqual('winrm', node.connection_type)
        self.assertEqual('vinz', node.username)
        self.assertEqual('/bin/python1', node.python_path)
        self.assertEqual('fish', node.shell_type)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(self.getJobFromHistory('check-job').result,
                         'SUCCESS')
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2)
        self.assertEqual(self.getJobFromHistory('check-job').node,
                         'debian-normal')

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_canceled_request(self):
        # Test that a canceled request is cleaned up
        self.hold_jobs_in_queue = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        queue = list(self.executor_api.queued())
        self.assertEqual(len(self.builds), 0)
        self.assertEqual(len(queue), 1)

        self.fake_gerrit.addEvent(A.getChangeAbandonedEvent())

        self.waitUntilSettled()
        self.assertHistory([])
        reqs = self.launcher.api.getNodesetRequests()
        self.assertEqual(0, len(reqs))
        self._waitForNoChildren('/zuul/nodeset/locks')
        self._waitForNoChildren('/zuul/nodeset/requests')

    @simple_layout('layouts/nodepool-empty-nodeset.yaml', enable_nodepool=True)
    def test_empty_nodeset(self):
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(self.getJobFromHistory('check-job').result,
                         'SUCCESS')
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2)
        self.assertEqual(self.getJobFromHistory('check-job').node,
                         None)

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_launcher_failover(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)

        with mock.patch(
            'zuul.driver.aws.awsendpoint.AwsProviderEndpoint._refresh'
        ) as refresh_mock:
            # Patch 'endpoint._refresh()' to return w/o updating
            refresh_mock.side_effect = lambda o: o
            self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
            for _ in iterate_timeout(10, "node is building"):
                nodes = self.launcher.api.nodes_cache.getItems()
                if not nodes:
                    continue
                if all(
                    n.create_state and
                    n.create_state[
                        "state"] == n.create_state_machine.INSTANCE_CREATING
                    for n in nodes
                ):
                    break
            self.launcher.stop()
            self.launcher.join()

            self.launcher = self.createLauncher()

        self.waitUntilSettled()
        self.assertEqual(self.getJobFromHistory('check-job').result,
                         'SUCCESS')
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2)
        self.assertEqual(self.getJobFromHistory('check-job').node,
                         'debian-normal')

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_launcher_connection_filter(self):
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)

        def launcher_score(name, item):
            return {
                'launcher-0': 1,
                'launcher-1': 0,
            }[name]

        self.useFixture(fixtures.MonkeyPatch(
            'zuul.zk.launcher.launcher_score',
            launcher_score))

        launcher2 = self.createLauncher(instance_id=1,
                                        connection_filter="nope")
        self.waitUntilSettled()

        # Ensure the first launcher is able to updateTenantProviders after
        # the new launcher has registered its connection filter.
        self.launcher.layout_updated_event.set()
        self.launcher.wake_event.set()
        time.sleep(1)
        self.waitUntilSettled()

        self.launcher.log = logging.getLogger("zuul.Launcher-0")
        launcher2.log = logging.getLogger("zuul.Launcher-1")

        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.State.FULFILLED, request.state)

        launcher2.stop()
        launcher2.join()

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_launcher_connection_filter_match(self):
        # Wait until everything is fully initialized before stopping
        # the launcher.
        self.waitUntilSettled()

        # Re-create the launcher with a connection filter
        self.launcher.stop()
        self.launcher.join()
        self.launcher = self.createLauncher(instance_id=1,
                                            connection_filter="nope, aws")
        self.waitUntilSettled()

        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.State.FULFILLED, request.state)
        self.waitUntilSettled()

    @simple_layout('layouts/launch-timeout.yaml', enable_nodepool=True)
    def test_launcher_launch_timeout(self):
        with mock.patch(
            'zuul.driver.aws.awsendpoint.AwsProviderEndpoint._refresh'
        ) as refresh_mock:
            # Patch 'endpoint._refresh()' to return w/o updating
            refresh_mock.side_effect = lambda o: o

            request = self.requestNodes(['debian-normal'])
            self.assertEqual(request.state, model.NodesetRequest.State.FAILED)

    @simple_layout('layouts/nodepool-untrusted-conf.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="test_external_id")
    def test_launcher_untrusted_project(self, mock_image_upload_run):
        # Test that we can add all the configuration in an untrusted
        # project (most other tests just do everything in a
        # config-project).

        in_repo_conf = textwrap.dedent(
            """
            - image: {'name': 'debian-local', 'type': 'zuul'}
            - flavor: {'name': 'normal'}
            - label:
                name: debian-local-normal
                image: debian-local
                flavor: normal
            - section:
                name: aws-us-east-1
                connection: aws
                region: us-east-1
                label-defaults:
                  boot-timeout: 120
                launch-timeout: 600
                object-storage:
                  bucket-name: zuul
                flavors:
                  - name: normal
                    instance-type: t3.medium
                images:
                  - name: debian-local
            - provider:
                name: aws-us-east-1-main
                section: aws-us-east-1
                labels:
                  - name: debian-local-normal
                    key-name: zuul
            """)

        file_dict = {'zuul.d/images.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertEqual(A.data['status'], 'MERGED')
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()

        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: build-debian-local-image
                image-build-name: debian-local
            - project:
                check:
                  jobs:
                    - build-debian-local-image
                gate:
                  jobs:
                    - build-debian-local-image
                image-build:
                  jobs:
                    - build-debian-local-image
            """)
        file_dict = {'zuul.d/image-jobs.yaml': in_repo_conf}
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B',
                                           files=file_dict)
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1'),
            dict(name='test-job', result='SUCCESS', changes='2,1'),
            dict(name='build-debian-local-image', result='SUCCESS',
                 changes='2,1'),
        ], ordered=False)
        self.assertEqual(B.data['status'], 'MERGED')
        self.fake_gerrit.addEvent(B.getChangeMergedEvent())
        self.waitUntilSettled()

        for _ in iterate_timeout(
                30, "scheduler and launcher to have the same layout"):
            if (self.scheds.first.sched.local_layout_state.get("tenant-one") ==
                self.launcher.local_layout_state.get("tenant-one")):
                break

        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/project',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='test-job', result='SUCCESS', changes='1,1'),
            dict(name='test-job', result='SUCCESS', changes='2,1'),
            dict(name='build-debian-local-image', result='SUCCESS',
                 changes='2,1'),
            dict(name='build-debian-local-image', result='SUCCESS',
                 ref='refs/heads/master'),
        ], ordered=False)
        for name in [
                'review.example.com%2Forg%2Fproject/debian-local',
        ]:
            artifacts = self._waitForArtifacts(name, 1)
            self.assertEqual('raw', artifacts[0].format)
            self.assertTrue(artifacts[0].validated)
            uploads = self.launcher.image_upload_registry.getUploadsForImage(
                name)
            self.assertEqual(1, len(uploads))
            self.assertEqual(artifacts[0].uuid, uploads[0].artifact_uuid)
            self.assertEqual("test_external_id", uploads[0].external_id)
            self.assertTrue(uploads[0].validated)

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_launcher_missing_label(self):
        ctx = self.createZKContext(None)
        labels = ["debian-normal", "debian-unavailable"]
        request = self.requestNodes(labels)
        self.assertEqual(request.state, model.NodesetRequest.State.FAILED)
        self.assertEqual(len(request.nodes), 0)

        request.delete(ctx)
        self.waitUntilSettled()

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_lost_nodeset_request(self):
        ctx = self.createZKContext(None)
        request = self.requestNodes(["debian-normal"])

        provider_nodes = []
        for node_id in request.nodes:
            provider_nodes.append(model.ProviderNode.fromZK(
                ctx, path=model.ProviderNode._getPath(node_id)))

        request.delete(ctx)
        self.waitUntilSettled()

        with testtools.ExpectedException(NoNodeError):
            # Request should be gone
            request.refresh(ctx)

        for pnode in provider_nodes:
            for _ in iterate_timeout(60, "node to be deallocated"):
                pnode.refresh(ctx)
                if pnode.request_id is None:
                    break

        request = self.requestNodes(["debian-normal"])
        self.waitUntilSettled()
        # Node should be re-used as part of the new request
        self.assertEqual(set(request.nodes), {n.uuid for n in provider_nodes})

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_multi_launcher_assignment(self):
        # Test that if a node is assigned to a request and our request
        # cache is not up to date, that we will wait for the cache
        # update and not deallocate the node.

        # Make the request cache event queue lockable
        lock = threading.Lock()
        orig_queue = self.launcher.api.requests_cache._event_queue
        orig_get = self.launcher.api.requests_cache._event_queue.get

        def get():
            with lock:
                try:
                    return orig_get(timeout=0.1)
                except queue.Empty:
                    pass
            # Yield to other threads (ie, this test)
            time.sleep(0.1)
            orig_queue.put(None)
            return orig_get()
        self.patch(self.launcher.api.requests_cache._event_queue,
                   'get', get)

        # Make the request cache max_zxid method controllable
        waiting_event = threading.Event()
        go_event = threading.Event()
        go_event.set()

        class MyCache(RequestCache):
            @property
            def max_zxid(self):
                waiting_event.set()
                go_event.wait()
                return super().max_zxid

        self.patch(self.launcher.api.requests_cache,
                   '__class__', MyCache)

        ctx = self.createZKContext(None)
        request = self.requestNodes(["debian-normal"])
        self.log.debug("Original request is %s", request.id)

        node_id = request.nodes[0]
        node = model.ProviderNode.fromZK(
            ctx, path=model.ProviderNode._getPath(node_id))

        request.delete(ctx)
        self.waitUntilSettled()

        with testtools.ExpectedException(NoNodeError):
            # Request should be gone
            request.refresh(ctx)

        for _ in iterate_timeout(10, "node to be deallocated"):
            node.refresh(ctx)
            if node.request_id is None:
                break

        # Lock request cache
        with lock:
            # Pause the launcher
            with self.launcher._run_lock:
                request = self.requestNodes(["debian-normal"], timeout=None)
                self.log.debug("Second request is %s", request.id)
                # Assign node to new request (unseen by launcher)
                with node.locked(ctx):
                    with node.activeContext(ctx):
                        node.assign(ctx,
                                    request_id=request.id,
                                    tenant_name=request.tenant_name)
                with request.locked(ctx):
                    with request.activeContext(ctx):
                        request.addProviderNode(node)
                # Wait for launcher node cache to update
                for _ in iterate_timeout(10, "node to update"):
                    request_ids = [n.request_id for n in
                                   self.launcher.api.nodes_cache.getItems()]
                    if request_ids == [request.id]:
                        break
                # Stop the launcher in waitForSync
                waiting_event.clear()
                go_event.clear()
            # Release launcher lock, advance to update check
            waiting_event.wait()
            # At this point we know the launcher loop is running and waiting
        # Release cache lock and allow the launcher to finish waiting
        go_event.set()
        # The launcher will resume; ensure node is still assigned
        with self.launcher._run_lock:
            node.refresh(ctx)
            self.assertEqual(request.id, node.request_id)
        self.waitUntilSettled()
        node.refresh(ctx)
        self.assertEqual(request.id, node.request_id)

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_multi_launcher_assignment_old_zk(self):
        # Test that if a node is assigned to a request and our request
        # cache is not up to date, that we will wait for the cache
        # update and not deallocate the node.

        # This performs the same test using waitForSync rather than
        # the new zxid feauter in ZK 3.9.
        # Make the request cache event queue lockable
        self.launcher.zk_client.cap_watched_event_zxid = False
        lock = threading.Lock()
        orig_queue = self.launcher.api.requests_cache._event_queue
        orig_get = self.launcher.api.requests_cache._event_queue.get

        def get():
            with lock:
                try:
                    return orig_get(timeout=0.1)
                except queue.Empty:
                    pass
            # Yield to other threads (ie, this test)
            time.sleep(0.1)
            orig_queue.put(None)
            return orig_get()
        self.patch(self.launcher.api.requests_cache._event_queue,
                   'get', get)

        # Make the request cache waitForSync method controllable
        waiting_event = threading.Event()
        go_event = threading.Event()
        go_event.set()
        orig_wait = self.launcher.api.requests_cache.waitForSync

        def wait(*args, **kw):
            waiting_event.set()
            go_event.wait()
            return orig_wait(*args, **kw)
        self.patch(self.launcher.api.requests_cache,
                   'waitForSync', wait)

        ctx = self.createZKContext(None)
        request = self.requestNodes(["debian-normal"])
        self.log.debug("Original request is %s", request.id)

        node_id = request.nodes[0]
        node = model.ProviderNode.fromZK(
            ctx, path=model.ProviderNode._getPath(node_id))

        request.delete(ctx)
        self.waitUntilSettled()

        with testtools.ExpectedException(NoNodeError):
            # Request should be gone
            request.refresh(ctx)

        for _ in iterate_timeout(10, "node to be deallocated"):
            node.refresh(ctx)
            if node.request_id is None:
                break

        # Lock request cache
        with lock:
            # Pause the launcher
            with self.launcher._run_lock:
                request = self.requestNodes(["debian-normal"], timeout=None)
                self.log.debug("Second request is %s", request.id)
                # Assign node to new request (unseen by launcher)
                with node.locked(ctx):
                    with node.activeContext(ctx):
                        node.assign(ctx,
                                    request_id=request.id,
                                    tenant_name=request.tenant_name)
                # Wait for launcher node cache to update
                for _ in iterate_timeout(10, "node to update"):
                    request_ids = [n.request_id for n in
                                   self.launcher.api.nodes_cache.getItems()]
                    if request_ids == [request.id]:
                        break
                # Stop the launcher in waitForSync
                waiting_event.clear()
                go_event.clear()
            # Release launcher lock, advance to waitForSync
            waiting_event.wait()
            # At this point we know the launcher loop is running and waiting
        # Release cache lock and allow the launcher to finish waiting
        go_event.set()
        # The launcher will resume; ensure node is still assigned
        with self.launcher._run_lock:
            node.refresh(ctx)
            self.assertEqual(request.id, node.request_id)
        self.waitUntilSettled()
        node.refresh(ctx)
        self.assertEqual(request.id, node.request_id)

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_delete_used_node(self):
        # Test that a node that somehow ended up in the used state
        # with no request is cleaned up.
        ctx = self.createZKContext(None)
        request = self.requestNodes(["debian-normal"])

        node_id = request.nodes[0]
        node = model.ProviderNode.fromZK(
            ctx, path=model.ProviderNode._getPath(node_id))

        request.delete(ctx)
        self.waitUntilSettled()

        with testtools.ExpectedException(NoNodeError):
            # Request should be gone
            request.refresh(ctx)

        for _ in iterate_timeout(60, "node to be deallocated"):
            node.refresh(ctx)
            if node.request_id is None:
                break

        with node.activeContext(ctx):
            self.log.debug("Set node to used")
            node.setState(node.State.USED)

        self.waitUntilSettled()
        for _ in iterate_timeout(60, "node to be deleted"):
            try:
                node.refresh(ctx)
            except NoNodeError:
                break

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    @okay_tracebacks('getResource')
    @mock.patch('zuul.launcher.server.Launcher.doesProviderHaveQuotaForLabel',
                return_value=True)
    @mock.patch('zuul.driver.aws.awsprovider.AwsProvider.getQuotaForLabel',
                return_value=model.QuotaInformation())
    def test_failed_node(self, mock_quota, mock_quota2):
        # Test a node failure outside of the create state machine
        ctx = self.createZKContext(None)
        request = self.requestNodes(["debian-invalid"])
        self.assertEqual(request.state, model.NodesetRequest.State.FAILED)
        provider_node_data = request.provider_node_data[0]
        self.assertEqual(len(provider_node_data['failed_providers']), 1)
        self.assertEqual(len(request.nodes), 1)

        provider_nodes = []
        for node_id in request.nodes:
            provider_nodes.append(model.ProviderNode.fromZK(
                ctx, path=model.ProviderNode._getPath(node_id)))

        request.delete(ctx)
        self.waitUntilSettled()

        for pnode in provider_nodes:
            for _ in iterate_timeout(60, "node to be deleted"):
                try:
                    pnode.refresh(ctx)
                except NoNodeError:
                    break

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    @mock.patch(
        'zuul.driver.aws.awsendpoint.AwsProviderEndpoint._createInstance',
        side_effect=Exception("Fake error"))
    @okay_tracebacks('_completeCreateInstance')
    def test_failed_node2(self, mock_createInstance):
        # Test a node failure inside the create state machine
        ctx = self.createZKContext(None)
        request = self.requestNodes(["debian-normal"])
        self.assertEqual(request.state, model.NodesetRequest.State.FAILED)
        provider_node_data = request.provider_node_data[0]
        self.assertEqual(len(provider_node_data['failed_providers']), 1)
        self.assertEqual(len(request.nodes), 1)

        provider_nodes = []
        for node_id in request.nodes:
            provider_nodes.append(model.ProviderNode.fromZK(
                ctx, path=model.ProviderNode._getPath(node_id)))

        request.delete(ctx)
        self.waitUntilSettled()

        for pnode in provider_nodes:
            for _ in iterate_timeout(60, "node to be deleted"):
                try:
                    pnode.refresh(ctx)
                except NoNodeError:
                    break

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    @mock.patch(
        'zuul.driver.aws.awsendpoint.AwsCreateStateMachine.advance',
        side_effect=Exception("Fake error"))
    @mock.patch(
        'zuul.driver.aws.awsendpoint.AwsDeleteStateMachine.advance',
        side_effect=Exception("Fake error"))
    @mock.patch('zuul.launcher.server.Launcher.DELETE_TIMEOUT', 1)
    @okay_tracebacks('_execute_mock_call')
    def test_failed_node3(self, mock_create, mock_delete):
        # Test a node failure inside both the create and delete state
        # machines
        ctx = self.createZKContext(None)
        request = self.requestNodes(["debian-normal"])
        self.assertEqual(request.state, model.NodesetRequest.State.FAILED)
        provider_node_data = request.provider_node_data[0]
        self.assertEqual(len(provider_node_data['failed_providers']), 1)
        self.assertEqual(len(request.nodes), 1)

        provider_nodes = []
        for node_id in request.nodes:
            provider_nodes.append(model.ProviderNode.fromZK(
                ctx, path=model.ProviderNode._getPath(node_id)))

        request.delete(ctx)
        self.waitUntilSettled()

        for pnode in provider_nodes:
            for _ in iterate_timeout(60, "node to be deleted"):
                try:
                    pnode.refresh(ctx)
                except NoNodeError:
                    break

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_accept_failure(self):
        # Test that we can resume accepting a request after an
        # interruption
        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.state, model.NodesetRequest.State.FULFILLED)
        self.assertEqual(len(request.nodes), 1)
        # Simulate an exception in _acceptRequest by reverting the
        # state to requested and running it again to see if we resume
        # without error.
        self.log.debug("Resetting request state")
        ctx = self.createZKContext(None)
        request.updateAttributes(
            ctx, state=model.NodesetRequest.State.REQUESTED)
        self.launcher.wake_event.set()
        self.log.debug("Waiting for request")
        for _ in iterate_timeout(60, "request to be accepted"):
            request.refresh(ctx)
            if request.state == model.NodesetRequest.State.FULFILLED:
                break
        self.assertEqual(len(request.nodes), 1)

    @simple_layout('layouts/launcher-nodeset-fallback.yaml',
                   enable_nodepool=True)
    @okay_tracebacks('getResource')
    def test_nodeset_fallback(self):
        # Test that nodeset fallback works
        self.executor_server.hold_jobs_in_build = True

        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        job = tenant.layout.getJob('check-job')
        alts = job.flattenNodesetAlternatives(tenant.layout)
        self.assertEqual(2, len(alts))
        self.assertEqual('debian-invalid', alts[0].nodes[('node',)].label)
        self.assertEqual('debian-normal', alts[1].nodes[('node',)].label)

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        build = self.getBuildByName('check-job')
        inv_path = os.path.join(build.jobdir.root, 'ansible', 'inventory.yaml')
        with open(inv_path, 'r') as f:
            inventory = yaml.safe_load(f)
        label = inventory['all']['hosts']['node']['nodepool']['label']
        self.assertEqual('debian-normal', label)

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1)
        self.assertNotIn('NODE_FAILURE', A.messages[0])
        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    @simple_layout('layouts/launcher-nodepool-paused-parent.yaml',
                   enable_nodepool=True)
    def test_nodepool_paused_parent(self):
        # Test that a nodeset request for a job with a launcher label
        # that depends on a paused job with a Nodepool legacy label is
        # fulfilled and the preferred provider correctly ignored.
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.executor_server.returnData(
            'parent-job', A,
            {'zuul': {'pause': True}}
        )

        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='parent-job', result='SUCCESS', changes='1,1'),
            dict(name='child-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    @mock.patch(
        'zuul.driver.aws.awsendpoint.AwsProviderEndpoint._createInstance',
        side_effect=exceptions.QuotaException)
    def test_quota_failure(self, mock_create):
        ctx = self.createZKContext(None)
        # This tests an unexpected quota error.
        # The request should never be fulfilled
        with testtools.ExpectedException(Exception):
            self.requestNodes(["debian-normal"])

        # We should have tried to build at least one node that was
        # marked as tempfail.
        requests = self.launcher.api.requests_cache.getItems()
        request = requests[0]
        self.assertTrue(isinstance(request.provider_node_data[0]['uuid'], str))
        # We can't assert anything about the node itself because it
        # will have been deleted, but we have asserted there was at
        # least an attempt.

        # Now explicitly delete the request to avoid exceptions in the
        # launcher caused by request processing attempting to relaunch the
        # node during launcher shutdown. The clean no exceptions in logs
        # check for unittests fails otherwise.
        with self.launcher._run_lock:
            request.delete(ctx)
            self.waitUntilSettled()

    @simple_layout('layouts/nodepool-multi-provider.yaml',
                   enable_nodepool=True)
    @driver_config('test_launcher', quotas={
        'instances': 2,
    })
    def test_provider_selection_spread(self):
        # Test that we spread quota use out among multiple providers
        self.waitUntilSettled()

        request1 = self.requestNodes(["debian-normal"])
        nodes1 = self.getNodes(request1)
        self.assertEqual(1, len(nodes1))

        request2 = self.requestNodes(["debian-normal"])
        nodes2 = self.getNodes(request2)
        self.assertEqual(1, len(nodes2))

        self.assertNotEqual(nodes1[0].provider, nodes2[0].provider)

    @simple_layout('layouts/nodepool-multi-provider.yaml',
                   enable_nodepool=True)
    @driver_config('test_launcher', quotas={
        'instances': 2,
    })
    def test_preferred_provider(self):
        # Test that we supply nodes from the requested provider if possible
        self.waitUntilSettled()

        request1 = self.requestNodes(["debian-normal"])
        nodes1 = self.getNodes(request1)
        self.assertEqual(1, len(nodes1))

        # Normally we would expect a second node to come from the
        # other provider (the test above this verifies that), but
        # let's specifically request the same provider.
        request2 = self.requestNodes(
            ["debian-normal"], provider=nodes1[0].provider)
        nodes2 = self.getNodes(request2)
        self.assertEqual(1, len(nodes2))

        self.assertEqual(nodes1[0].provider, nodes2[0].provider)

    @simple_layout('layouts/nodepool.yaml',
                   enable_nodepool=True)
    @driver_config('test_launcher', quotas={
        'instances': 2,
    })
    @okay_tracebacks('_listImportSnapshotTasks')
    def test_quota_external_usage(self):
        # Test that we spread quota use out among multiple providers
        self.waitUntilSettled()

        # Create 2 unmanaged instances that eat up our quota
        ec2_client = boto3.client('ec2', region_name='us-east-1')
        ec2_client.run_instances(
            ImageId="ami-12c6146b", MinCount=2, MaxCount=2,
        )
        self.launcher._provider_quota_cache.clear()

        # The cleanup worker is responsible for recording external
        # usage in the quota cache
        self.launcher.cleanup_worker._run()

        # We expect a timeout waiting for nodes, and no other exceptions
        with testtools.ExpectedException(Exception):
            self.requestNodes(["debian-normal"])

    @simple_layout('layouts/resource-limits.yaml',
                   enable_nodepool=True)
    @driver_config('test_launcher', quotas={
        'instances': 3,
    })
    @okay_tracebacks('_listImportSnapshotTasks')
    def test_resource_limits_external_usage(self):
        self.waitUntilSettled()

        # Create 2 unmanaged instances that only leave one instance
        # as available quota.
        ec2_client = boto3.client('ec2', region_name='us-east-1')
        ec2_client.run_instances(
            ImageId="ami-12c6146b", MinCount=2, MaxCount=2,
        )
        self.launcher._provider_quota_cache.clear()

        # The cleanup worker is responsible for recording external
        # usage in the quota cache
        self.launcher.cleanup_worker._run()

        # We can still get a node since we've not reached
        # the endpoint quota limit.
        self.requestNodes(["debian-normal"])

        # We expect a timeout waiting for nodes, as the
        # endpoint has reached the instance limit.
        with testtools.ExpectedException(Exception):
            self.requestNodes(["debian-normal"])

    @simple_layout('layouts/resource-limits.yaml',
                   enable_nodepool=True)
    @driver_config('test_launcher', quotas={
        'instances': 3,
    })
    @okay_tracebacks('_listImportSnapshotTasks')
    def test_resource_limits_providers(self):
        self.waitUntilSettled()

        # Create an unmanaged instances
        ec2_client = boto3.client('ec2', region_name='us-east-1')
        ec2_client.run_instances(
            ImageId="ami-12c6146b", MinCount=1, MaxCount=1,
        )
        self.launcher._provider_quota_cache.clear()

        # The cleanup worker is responsible for recording external
        # usage in the quota cache
        self.launcher.cleanup_worker._run()

        # We can still get a node since we've not reached
        # the endpoint quota limit.
        self.requestNodes(["debian-large"])

        # We expect a timeout waiting for nodes, as the
        # PROVIDER has reached the instance limit.
        with testtools.ExpectedException(Exception):
            self.requestNodes(["debian-large"])

        # We can still get a node from a different provider
        self.requestNodes(["debian-normal"])

        # We expect a timeout waiting for nodes, as we
        # have reached the ENDPOINT instance limit.
        with testtools.ExpectedException(Exception):
            self.requestNodes(["debian-normal"])

    @simple_layout('layouts/nodepool.yaml',
                   enable_nodepool=True)
    @driver_config('test_launcher', quotas={
        'instances': 0,
    })
    def test_quota_insufficient_capacity(self):
        # Test that we fail requests which are impossible to satisfy
        self.waitUntilSettled()
        request = self.requestNodes(["debian-normal"])
        self.assertEqual(request.state, model.NodesetRequest.State.FAILED)

    @simple_layout('layouts/nodepool-nodescan.yaml', enable_nodepool=True)
    @okay_tracebacks('_checkNodescanRequest')
    def test_nodescan_failure(self):
        # Test a nodescan failure
        self.useFixture(NodescanFixture(transport_fail=True))

        ctx = self.createZKContext(None)
        request = self.requestNodes(["debian-normal"], timeout=30)
        self.assertEqual(request.state, model.NodesetRequest.State.FAILED)
        provider_node_data = request.provider_node_data[0]
        self.assertEqual(len(provider_node_data['failed_providers']), 1)
        self.assertEqual(len(request.nodes), 1)

        provider_nodes = []
        for node_id in request.nodes:
            provider_nodes.append(model.ProviderNode.fromZK(
                ctx, path=model.ProviderNode._getPath(node_id)))

        request.delete(ctx)
        self.waitUntilSettled()

        for pnode in provider_nodes:
            for _ in iterate_timeout(60, "node to be deleted"):
                try:
                    pnode.refresh(ctx)
                except NoNodeError:
                    break

    @simple_layout('layouts/nodepool-nodescan.yaml', enable_nodepool=True)
    @okay_tracebacks('_checkNodescanRequest')
    def test_nodescan_success(self):
        # Test a normal launch with a nodescan
        self.useFixture(NodescanFixture())

        ctx = self.createZKContext(None)
        request = self.requestNodes(["debian-normal"])
        self.assertEqual(request.state, model.NodesetRequest.State.FULFILLED)
        provider_node_data = request.provider_node_data[0]
        self.assertEqual(len(provider_node_data['failed_providers']), 0)
        self.assertEqual(len(request.nodes), 1)

        node = model.ProviderNode.fromZK(
            ctx, path=model.ProviderNode._getPath(request.nodes[0]))
        self.assertEqual(['fake key fake base64'], node.host_keys)

    @simple_layout('layouts/nodepool-image-hash.yaml', enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @return_data(
        'build-ubuntu-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.ubuntu_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="ami-1e749f67")
    def test_image_hash(self, mock_uploadimage):
        # Test that we avoid uploads of duplicate image hashes
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local', 'ubuntu-local'],
        )
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)

        for _ in iterate_timeout(60, "upload to complete"):
            uploads = self.launcher.image_upload_registry.getUploadsForImage(
                'review.example.com%2Forg%2Fcommon-config/debian-local')
            self.assertEqual(1, len(uploads))
            pending1 = [u for u in uploads if u.external_id is None]

            uploads = self.launcher.image_upload_registry.getUploadsForImage(
                'review.example.com%2Forg%2Fcommon-config/ubuntu-local')
            self.assertEqual(2, len(uploads))
            pending2 = [u for u in uploads if u.external_id is None]
            if not (pending1 or pending2):
                break
        # The above ensures that we have 1 debian upload (its hash is
        # identical) and 2 ubuntu uploads (different hashes).

    @simple_layout('layouts/nodepool-image.yaml', enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @return_data(
        'build-ubuntu-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.ubuntu_return_data,
    )
    # Use an existing image id since the upload methods aren't
    # implemented in boto; the actualy upload process will be tested
    # in test_aws_driver.
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="ami-1e749f67")
    def test_image_build_node_lifecycle(self, mock_uploadimage):
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local', 'ubuntu-local'],
        )
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)

        for _ in iterate_timeout(60, "upload to complete"):
            uploads = self.launcher.image_upload_registry.getUploadsForImage(
                'review.example.com%2Forg%2Fcommon-config/debian-local')
            self.assertEqual(1, len(uploads))
            pending = [u for u in uploads if u.external_id is None]
            if not pending:
                break

        nodeset = model.NodeSet()
        nodeset.addNode(model.Node("node", "debian-local-normal"))

        ctx = self.createZKContext(None)
        request = self.requestNodes([n.label for n in nodeset.getNodes()])

        client = LauncherClient(self.zk_client, None)
        request = client.getRequest(request.uuid)

        self.assertEqual(request.state, model.NodesetRequest.State.FULFILLED)
        self.assertEqual(len(request.nodes), 1)

        client.acceptNodeset(request, nodeset)
        self.waitUntilSettled()

        with testtools.ExpectedException(NoNodeError):
            # Request should be gone
            request.refresh(ctx)

        for node in nodeset.getNodes():
            pnode = node._provider_node
            self.assertIsNotNone(pnode)
            self.assertTrue(pnode.hasLock())

        client.useNodeset(nodeset)
        self.waitUntilSettled()

        for node in nodeset.getNodes():
            pnode = node._provider_node
            self.assertTrue(pnode.hasLock())
            self.assertTrue(pnode.state, pnode.State.IN_USE)

        client.returnNodeset(nodeset)
        self.waitUntilSettled()

        for node in nodeset.getNodes():
            pnode = node._provider_node
            self.assertFalse(pnode.hasLock())
            self.assertTrue(pnode.state, pnode.State.USED)

            for _ in iterate_timeout(60, "node to be deleted"):
                try:
                    pnode.refresh(ctx)
                except NoNodeError:
                    break

    @simple_layout('layouts/launcher-image-lifecycle/phase1.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="ami-1e749f67")
    def test_image_delete_lifecycle(self, mock_uploadimage):
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled("phase1")
        # Initialize some values for later

        class FakeNode:
            pass

        node = FakeNode
        node.image_upload_uuid = None
        node.label = 'debian-local-normal'
        image_cname = 'review.example.com%2Forg%2Fcommon-config/debian-local'
        image_id = "ami-1e749f67"

        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
        ], ordered=False)
        uploads = self._waitForUploads(image_cname)
        # At this point the image should be in one provider only
        self.assertEqual(1, len(uploads))
        provider_main = self.launcher._getProvider(
            'tenant-one', 'aws-us-east-1-main')
        self.assertEqual(
            image_id, self.launcher.getImageExternalId(node, provider_main))

        # Add the image to the other providers
        self.commitConfigUpdate(
            'org/common-config',
            'layouts/launcher-image-lifecycle/phase2.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitForLauncherLayoutSync()

        self.waitUntilSettled("phase2")
        uploads = self._waitForUploads(image_cname)
        self.assertEqual(1, len(uploads))
        provider_main = self.launcher._getProvider(
            'tenant-one', 'aws-us-east-1-main')
        provider_same = self.launcher._getProvider(
            'tenant-one', 'aws-us-east-1-same')
        provider_diff = self.launcher._getProvider(
            'tenant-one', 'aws-us-east-1-different')
        self.assertEqual(
            image_id, self.launcher.getImageExternalId(node, provider_main))
        self.assertEqual(
            image_id, self.launcher.getImageExternalId(node, provider_same))
        with testtools.ExpectedException(Exception, "No image found"):
            self.assertEqual(
                image_id, self.launcher.getImageExternalId(
                    node, provider_diff))

        # Update the config for the main provider
        self.commitConfigUpdate(
            'org/common-config',
            'layouts/launcher-image-lifecycle/phase3.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitForLauncherLayoutSync()

        self.waitUntilSettled("phase3")
        uploads = self._waitForUploads(image_cname)
        self.assertEqual(1, len(uploads))
        provider_main = self.launcher._getProvider(
            'tenant-one', 'aws-us-east-1-main')
        provider_same = self.launcher._getProvider(
            'tenant-one', 'aws-us-east-1-same')
        provider_diff = self.launcher._getProvider(
            'tenant-one', 'aws-us-east-1-different')
        self.assertEqual(
            image_id, self.launcher.getImageExternalId(node, provider_main))
        self.assertEqual(
            image_id, self.launcher.getImageExternalId(node, provider_same))
        with testtools.ExpectedException(Exception, "No image found"):
            self.assertEqual(
                image_id, self.launcher.getImageExternalId(
                    node, provider_diff))

        # Remove the image from the other providers
        self.commitConfigUpdate(
            'org/common-config',
            'layouts/launcher-image-lifecycle/phase4.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitForLauncherLayoutSync()

        self.waitUntilSettled("phase4")
        uploads = self._waitForUploads(image_cname)
        self.assertEqual(1, len(uploads))
        provider_main = self.launcher._getProvider(
            'tenant-one', 'aws-us-east-1-main')
        self.assertEqual(
            image_id, self.launcher.getImageExternalId(node, provider_main))

        # Remove the image from all providers
        self.commitConfigUpdate(
            'org/common-config',
            'layouts/launcher-image-lifecycle/phase5.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitForLauncherLayoutSync()

        self.waitUntilSettled("phase5")
        uploads = self._waitForUploads(image_cname, count=0)

    @simple_layout('layouts/nodepool.yaml',
                   enable_nodepool=True)
    def test_snapshot(self):
        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'])

        def advance(self):
            self.complete = True
            return "test-external-id"
        self.patch(zuul.driver.aws.awsendpoint.AwsSnapshotStateMachine,
                   'advance',
                   advance)

        # This is the executor's copy of the node
        node = model.ProviderNode.fromZK(
            ctx, path=model.ProviderNode._getPath(request.nodes[0]))

        with node.locked(ctx):
            request.delete(ctx)
            node.createSnapshot(ctx)
            with node.activeContext(ctx):
                self.log.debug("Set node to snapshot")
                node.setState(node.State.SNAPSHOT)

            for _ in iterate_timeout(60, "snapshot"):
                if node.snapshot.complete:
                    break
                node.snapshot.refresh(ctx)

            self.assertTrue(node.snapshot.complete)
            self.assertEqual("test-external-id", node.snapshot.external_id)
            with node.activeContext(ctx):
                self.log.debug("Set node to used")
                node.setState(node.State.USED)

        for _ in iterate_timeout(60, "node to be deleted"):
            try:
                node.refresh(ctx)
            except NoNodeError:
                break

    @simple_layout('layouts/nodepool-snapshot-timeout.yaml',
                   enable_nodepool=True)
    @okay_tracebacks('_checkNodeSnapshot')
    def test_snapshot_timeout(self):
        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'])

        def advance(self):
            self.state = "not start"
        self.patch(zuul.driver.aws.awsendpoint.AwsSnapshotStateMachine,
                   'advance',
                   advance)

        # This is the executor's copy of the node
        node = model.ProviderNode.fromZK(
            ctx, path=model.ProviderNode._getPath(request.nodes[0]))

        with node.locked(ctx):
            request.delete(ctx)
            node.createSnapshot(ctx)
            with node.activeContext(ctx):
                self.log.debug("Set node to snapshot")
                node.setState(node.State.SNAPSHOT)

            for _ in iterate_timeout(60, "snapshot"):
                if node.snapshot.complete:
                    break
                node.snapshot.refresh(ctx)

            self.assertTrue(node.snapshot.complete)
            self.assertEqual(None, node.snapshot.external_id)
            with node.activeContext(ctx):
                self.log.debug("Set node to used")
                node.setState(node.State.USED)

        for _ in iterate_timeout(60, "node to be deleted"):
            try:
                node.refresh(ctx)
            except NoNodeError:
                break

    @simple_layout('layouts/nodepool.yaml',
                   enable_nodepool=True)
    @driver_config('test_launcher', quotas={
        'instances': 1,
    })
    def test_relative_priority(self):
        self.waitUntilSettled()

        client = LauncherClient(self.zk_client, None)
        # Create a request so the following requests can't be fulfilled
        # due to instance quota.
        request0 = self.requestNodes(["debian-normal"])
        nodes0 = self.getNodes(request0)
        self.assertEqual(1, len(nodes0))

        # Make sure the next requests always have current quota info
        self.launcher._provider_limits_cache = cachetools.TTLCache(
            maxsize=8192, ttl=0)
        self.launcher._provider_quota_cache = cachetools.TTLCache(
            maxsize=8192, ttl=0)

        requests = []
        ctx = self.createZKContext(None)
        for _ in range(2):
            request = model.NodesetRequest.new(
                ctx,
                tenant_name="tenant-one",
                pipeline_name="test",
                buildset_uuid=uuid.uuid4().hex,
                job_uuid=uuid.uuid4().hex,
                job_name="foobar",
                labels=["debian-normal"],
                priority=100,
                request_time=time.time(),
                zuul_event_id=uuid.uuid4().hex,
                span_info=None,
            )
            requests.append(request)

        # Allow the main loop to run to verify that we defer the
        # requests
        time.sleep(2)
        # Revise relative priority, so that the last requests has
        # the highest relative priority.
        with self.launcher._run_lock:
            request1_p2, request2_p1 = requests
            client.reviseRequest(request1_p2, relative_priority=2)
            client.reviseRequest(request2_p1, relative_priority=1)

            # Delete the initial request to free up the instance
            request0.delete(ctx)

        # Last request should be fulfilled
        for _ in iterate_timeout(10, "request to be fulfilled"):
            request2_p1.refresh(ctx)
            if request2_p1.state == request2_p1.State.FULFILLED:
                break

        # Lower priority request should not be fulfilled
        request1_p2.refresh(ctx)
        self.assertEqual(request1_p2.State.REQUESTED, request1_p2.state)

    @simple_layout('layouts/launcher-noop.yaml',
                   enable_nodepool=True)
    def test_noop_job(self):
        # Test that nodeset fallback works
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1)
        self.assertIn('Build succeeded', A.messages[0])
        self.assertHistory([])
        ids = self.scheds.first.sched.launcher.getRequestIds()
        self.assertEqual(0, len(ids))

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_autohold(self):
        self.scheds.first.sched.autohold(
            'tenant-one', 'review.example.com/org/project', 'check-job',
            ".*", "reason text", 1, None)

        # There should be a record in ZooKeeper
        request_list = self.sched_zk_nodepool.getHoldRequests()
        self.assertEqual(1, len(request_list))
        request = self.sched_zk_nodepool.getHoldRequest(
            request_list[0])
        self.assertIsNotNone(request)
        self.assertEqual('tenant-one', request.tenant)
        self.assertEqual('review.example.com/org/project', request.project)
        self.assertEqual('check-job', request.job)
        self.assertEqual('reason text', request.reason)
        self.assertEqual(1, request.max_count)
        self.assertEqual(0, request.current_count)
        self.assertEqual([], request.nodes)

        # First check that successful jobs do not autohold
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))

        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1)
        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        # Check for a held node
        held_node = None
        self.launcher.api.nodes_cache.waitForSync()
        for node in self.launcher.api.nodes_cache.getItems():
            if node.state == node.State.HOLD:
                held_node = node
                break
        self.assertIsNone(held_node)

        # Hold in build to check the stats
        self.executor_server.hold_jobs_in_build = True

        # Now test that failed jobs are autoheld

        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        self.executor_server.failJob('check-job', B)
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))

        self.waitUntilSettled()

        # Get the build request object
        build = list(self.getCurrentBuilds())[0]

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(B.data['status'], 'NEW')
        self.assertEqual(B.reported, 1)

        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
            dict(name='check-job', result='FAILURE', changes='2,1'),
        ], ordered=False)
        self.assertTrue(build.held)

        # Check for a held node
        held_node = None
        self.launcher.api.nodes_cache.waitForSync()
        for node in self.launcher.api.nodes_cache.getItems():
            if node.state == node.State.HOLD:
                held_node = node
                break
        self.assertIsNotNone(held_node)
        self.assertEqual(held_node.comment, "reason text")

        # The hold request current_count should have incremented
        # and we should have recorded the held node ID.
        request2 = self.sched_zk_nodepool.getHoldRequest(
            request.id)
        self.assertEqual(request.current_count + 1, request2.current_count)
        self.assertEqual(1, len(request2.nodes))
        self.assertEqual(1, len(request2.nodes[0]["nodes"]))

        # Another failed change should not hold any more nodes
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        self.executor_server.failJob('check-job', C)
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(C.data['status'], 'NEW')
        self.assertEqual(C.reported, 1)
        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
            dict(name='check-job', result='FAILURE', changes='2,1'),
            dict(name='check-job', result='FAILURE', changes='3,1'),
        ], ordered=False)

        held_nodes = 0
        self.launcher.api.nodes_cache.waitForSync()
        for node in self.launcher.api.nodes_cache.getItems():
            if node.state == node.State.HOLD:
                held_nodes += 1
        self.assertEqual(held_nodes, 1)

        # request current_count should not have changed
        request3 = self.sched_zk_nodepool.getHoldRequest(
            request2.id)
        self.assertEqual(request2.current_count, request3.current_count)

        # Deleting hold request should set held nodes to used
        client = LauncherClient(self.zk_client, None)
        self.sched_zk_nodepool.deleteHoldRequest(request3, client)

        held_nodes = 0
        self.launcher.api.nodes_cache.waitForSync()
        for node in self.launcher.api.nodes_cache.getItems():
            if node.state == node.State.HOLD:
                held_nodes += 1
        self.assertEqual(held_nodes, 0)

        for _ in iterate_timeout(60, "node to be deleted"):
            if len(self.launcher.api.nodes_cache.getItems()) == 0:
                break

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_request_completion_event(self):
        # Test the event handling in request processing:
        # if the launcher crashes before the completion event is sent,
        # it should be sent by the next launcher to handle it.
        self.waitUntilSettled()
        tenant = 'tenant-one'
        pipeline = 'check'
        timeout = 10
        result_queue = PipelineResultEventQueue(
            self.zk_client, tenant, pipeline)

        request = self.requestNodes(["debian-normal"])
        self.assertEqual(request.State.FULFILLED, request.state)
        self.assertEqual(request.EventState.COMPLETE, request.event_state)

        # Hold the scheduler lock, reset the event state flag, and
        # ensure the scheduler sees another event.
        with (self.scheds.first.sched.run_handler_lock,
              pipeline_lock(self.zk_client, tenant, pipeline)):
            # First ensure the launcher doesn't send another event;
            # send it through its processing loop.
            self.launcher.wake_event.set()
            for _ in iterate_timeout(timeout, "launcher to settle"):
                if not self.launcher.wake_event.isSet():
                    break
            with self.launcher._test_lock:
                pass
            # The launcher should be back at the start of the loop now.
            # Make sure our queue is still empty
            result_events = list(result_queue)
            self.assertEqual(0, len(result_events))
            ctx = self.createZKContext(None)
            request.updateAttributes(ctx, event_state=0)
            result_events = list(result_queue)
            for _ in iterate_timeout(
                    timeout, "nodeset request to be fulfilled"):
                result_events = list(result_queue)
                if result_events:
                    for event in result_events:
                        # Remove event(s) from queue
                        result_queue.ack(event)
                    break

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_building_node_reassignment(self):
        # Test that a request for a building node gets assigned to a
        # new request.
        ctx = self.createZKContext(None)
        with mock.patch(
            'zuul.driver.aws.awsendpoint.AwsProviderEndpoint._refresh'
        ) as refresh_mock:
            # Patch 'endpoint._refresh()' to return w/o updating
            refresh_mock.side_effect = lambda o: o
            request1 = self.requestNodes(['debian-normal'], timeout=0)
            for _ in iterate_timeout(10, "node is building"):
                nodes = self.launcher.api.nodes_cache.getItems()
                if len(nodes) != 1:
                    continue
                node = nodes[0]
                if node.state == node.State.BUILDING:
                    break

            self.assertEqual(request1.uuid, node.request_id)
            self.log.debug("Delete request")
            request1.delete(ctx)

            for _ in iterate_timeout(10, "node is unassigned"):
                nodes = self.launcher.api.nodes_cache.getItems()
                if len(nodes) != 1:
                    continue
                node = nodes[0]

                if (node.request_id is None):
                    break

            request2 = self.requestNodes(['debian-normal'], timeout=0)
            for _ in iterate_timeout(10, "node is reassigned"):
                nodes = self.launcher.api.nodes_cache.getItems()
                if len(nodes) != 1:
                    continue
                node = nodes[0]

                if (node.request_id == request2.uuid):
                    break

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_failed_node_reassignment(self):
        # Test that a request with a failed node can get an unassigned
        # node assigned to it.
        ctx = self.createZKContext(None)

        # Create a node that will be used later as our unassigned
        # node.  For the moment though, it will be assigned to
        # request1.
        request1 = self.requestNodes(['debian-normal'])
        self.assertEqual(request1.State.FULFILLED, request1.state)
        nodes = self.launcher.api.nodes_cache.getItems()
        node1 = nodes[0]
        self.assertEqual(request1.uuid, node1.request_id)

        with mock.patch(
            'zuul.driver.aws.awsendpoint.AwsProviderEndpoint._refresh'
        ) as refresh_mock:
            # Patch 'endpoint._refresh()' to return w/o updating
            refresh_mock.side_effect = lambda o: o
            request2 = self.requestNodes(['debian-normal'], timeout=0)
            for _ in iterate_timeout(10, "node is building"):
                nodes = self.launcher.api.nodes_cache.getItems()
                if len(nodes) != 2:
                    continue
                node2 = nodes[1]
                if node2.state == node2.State.BUILDING:
                    break
            self.assertEqual(request2.uuid, node2.request_id)

            self.log.debug("Delete original request")
            request1.delete(ctx)

            for _ in iterate_timeout(60, "node is unassigned"):
                nodes = self.launcher.api.nodes_cache.getItems()
                node = [n for n in nodes if n.uuid == node1.uuid][0]
                if (node.uuid == node1.uuid and
                    node.request_id is None):
                    break

            # Stop the launcher so we avoid racing updates of the node
            with self.launcher._test_lock:
                with node2.activeContext(ctx):
                    node2.setState(node2.State.FAILED)

            for _ in iterate_timeout(60, "node is reassigned"):
                nodes = self.launcher.api.nodes_cache.getItems()
                node = [n for n in nodes if n.uuid == node1.uuid][0]
                if (node.uuid == node1.uuid and
                    node.request_id == request2.uuid):
                    break

            for _ in iterate_timeout(60, "request is fulfilled"):
                request2.refresh(ctx)
                if request2.state == request2.State.FULFILLED:
                    break


class TestLauncherTenantLimits(LauncherBaseTestCase):
    # We use a multi-tenant config here to make sure we don't end up
    # using provider objects from different tenants during provider
    # selection.
    tenant_config_file = "config/launcher-multi-tenant/main.yaml"

    def test_label_max_nodes_single_tenant(self):
        self.commitConfigUpdate(
            'common-config',
            'config/launcher-multi-tenant/git/'
            'common-config/zuul-max-nodes.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitForLauncherLayoutSync()

        ctx = self.createZKContext(None)

        # Request 2 nodes that will reach our tenant max-nodes
        request1 = self.requestNodes(['debian-normal'])
        self.assertEqual(request1.State.FULFILLED, request1.state)

        request2 = self.requestNodes(['debian-normal'])
        self.assertEqual(request2.State.FULFILLED, request2.state)
        node2 = model.ProviderNode.fromZK(
            ctx, path=model.ProviderNode._getPath(request2.nodes[0]))

        # Request one more which will wait for capacity
        request3 = self.requestNodes(['debian-normal'], timeout=0)
        time.sleep(5)
        request3.refresh(ctx)
        self.assertEqual(request3.State.REQUESTED, request3.state)

        # Delete the second node
        with node2.locked(ctx):
            request2.delete(ctx)
            with node2.activeContext(ctx):
                node2.setState(node2.State.USED)

        # Wait for the third to be created once we have capacity
        for _ in iterate_timeout(60, "request is fulfilled"):
            request3.refresh(ctx)
            if request3.state == request2.State.FULFILLED:
                break

    def test_label_max_nodes_two_tenants(self):
        self.commitConfigUpdate(
            'common-config',
            'config/launcher-multi-tenant/git/'
            'common-config/zuul-max-nodes.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitForLauncherLayoutSync()

        # Request 2 nodes that will reach our tenant max-nodes
        request1 = self.requestNodes(['debian-normal'])
        self.assertEqual(request1.State.FULFILLED, request1.state)

        request2 = self.requestNodes(['debian-normal'])
        self.assertEqual(request2.State.FULFILLED, request2.state)

        # Request one more in a different tenant, it should be fulfilled
        request3 = self.requestNodes(['debian-normal'], tenant="tenant-two")
        self.assertEqual(request3.State.FULFILLED, request3.state)


class TestLauncherLocality(LauncherBaseTestCase):
    # We use a multi-tenant config here to make sure we don't end up
    # using provider objects from different tenants during provider
    # selection.
    tenant_config_file = "config/launcher-multi-tenant/main.yaml"

    @driver_config('test_launcher', quotas={
        'instances': 3,
    })
    def test_provider_selection_locality(self):
        # Test that we use the same provider for multiple nodes within
        # a request if possible.
        self.waitUntilSettled()

        request1 = self.requestNodes(["debian-normal", "debian-normal"])
        nodes1 = self.getNodes(request1)
        self.assertEqual(2, len(nodes1))

        # These can be served from either provider; both should be
        # assigned to the same one.
        self.assertEqual(nodes1[0].provider, nodes1[1].provider)

        request2 = self.requestNodes(["debian-large", "debian-small"])
        nodes2 = self.getNodes(request2)
        self.assertEqual(2, len(nodes2))

        # These are served by different providers, so they should be
        # different.
        self.assertNotEqual(nodes2[0].provider, nodes2[1].provider)
        # Sanity check since we know the actual providers.
        self.assertEqual('review.example.com%2Fcommon-config/'
                         'aws-us-west-1-main', nodes2[0].provider)
        self.assertEqual('review.example.com%2Fcommon-config/'
                         'aws-us-east-1-main', nodes2[1].provider)

    @driver_config('test_launcher', quotas={
        'instances': 2,
    })
    @okay_tracebacks('_checkNodescanRequest')
    def test_provider_selection_locality_failure(self):
        # Test that we use the same provider for multiple nodes within
        # a request if possible.
        self.waitUntilSettled()

        orig_advance = zuul.launcher.server.NodescanRequest.advance
        failed_count = 0

        def my_advance(*args, **kw):
            nonlocal failed_count
            if failed_count > 0:
                return orig_advance(*args, **kw)
            failed_count += 1
            raise Exception("Test exception")

        with mock.patch.object(
            zuul.launcher.server.NodescanRequest, 'advance', my_advance
        ):
            request1 = self.requestNodes(["debian-normal", "debian-normal"],
                                         tenant="tenant-two",
                                         timeout=30)
        nodes1 = self.getNodes(request1)
        nodes1 = [n for n in nodes1 if n.state == n.State.READY]
        self.assertEqual(2, len(nodes1))

        # These can be served from either provider; both should be
        # assigned to the same one.
        self.assertEqual(nodes1[0].provider, nodes1[1].provider)

        # Ensure that we didn't leave an extra ready node
        self.assertEqual(2, len(self.launcher.api.nodes_cache.getItems()))

    @okay_tracebacks('_checkNodescanRequest')
    def test_provider_selection_locality_exhaustion(self):
        # Test that we use the same provider for multiple nodes within
        # a request if possible.  This exhausts the launch attempts on
        # one provider and ensures that we shift both nodes to the
        # second provider.

        # This test requests a pair of nodes, and causes the first
        # attempt of both nodes to fail; they will both be retried on
        # the same provider.  The second attempt of the first node
        # will also fail, causing it to be retried on the other
        # provider.  The second attempt of the second node succeeds,
        # but is nonetheless retried on the second provider in order
        # to preserve locality.
        self.waitUntilSettled()

        orig_advance = zuul.launcher.server.NodescanRequest.advance
        failed_count_by_node = {}

        def my_advance(*args, **kw):
            ns_request = args[0]
            failed_nodes = len(failed_count_by_node.keys())
            if failed_nodes > 2:
                return orig_advance(*args, **kw)
            failed_count = failed_count_by_node.get(
                ns_request.node.uuid, 0)
            failed_count_by_node[ns_request.node.uuid] = failed_count + 1
            raise Exception("Test exception")

        with mock.patch.object(
            zuul.launcher.server.NodescanRequest, 'advance', my_advance
        ):
            request1 = self.requestNodes(["debian-normal", "debian-normal"],
                                         tenant="tenant-two",
                                         timeout=30)
        nodes1 = self.getNodes(request1)
        nodes1 = [n for n in nodes1 if n.state == n.State.READY]
        nodes1 = [n for n in nodes1 if n.uuid in request1.nodes]
        self.assertEqual(2, len(nodes1))

        # These can be served from either provider; both should be
        # assigned to the same one.
        self.assertEqual(nodes1[0].provider, nodes1[1].provider)

        # There should be 3 total ready nodes, one unattached, so wait
        # for the failed nodes to be deleted to avoid test cleanup
        # races.
        for _ in iterate_timeout(60, "extra nodes to be deleted"):
            if len(self.launcher.api.nodes_cache.getItems()) == 3:
                break

    @okay_tracebacks('_checkNodescanRequest')
    def test_provider_selection_locality_exhaustion_failure(self):
        # Test that we use the same provider for multiple nodes within
        # a request if possible.  This exhausts the launch attempts on
        # both providers and ensures that we fail the request.
        self.waitUntilSettled()

        def my_advance(*args, **kw):
            raise Exception("Test exception")

        with mock.patch.object(
            zuul.launcher.server.NodescanRequest, 'advance', my_advance
        ):
            request1 = self.requestNodes(["debian-normal", "debian-normal"],
                                         tenant="tenant-two",
                                         timeout=30)
        self.assertEqual(request1.state, model.NodesetRequest.State.FAILED)

        # The last two nodes will still be referenced by the request,
        # so delete the request to release them.
        ctx = self.createZKContext(None)
        request1.delete(ctx)
        self.waitUntilSettled()

        # All nodes failed, so all should be deleted, but we may have
        # one ready node from the last attempt for the second
        # position; wait everything to stabilize to avoid test cleanup
        # races.
        for _ in iterate_timeout(60, "extra nodes to be deleted"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if len(nodes) == 0:
                break
            if all(n.state == n.State.READY for n in nodes):
                break

    @driver_config('test_launcher', quotas={
        'instances': 2,
    })
    def test_provider_selection_locality_multi_ready(self):
        # Test that we use the same provider when multiple ready nodes
        # from different providers are available.
        self.waitUntilSettled()

        request1 = self.requestNodes(["debian-normal"])
        nodes1 = self.getNodes(request1)
        self.assertEqual(1, len(nodes1))

        request2 = self.requestNodes(["debian-normal"])
        nodes2 = self.getNodes(request2)
        self.assertEqual(1, len(nodes2))

        self.assertNotEqual(nodes1[0].provider, nodes2[0].provider)

        ctx = self.createZKContext(None)
        request1.delete(ctx)
        request2.delete(ctx)
        self.waitUntilSettled()

        nodes = self.launcher.api.nodes_cache.getItems()
        for node in nodes:
            if not len(nodes) == 2:
                continue
            for _ in iterate_timeout(60, "node to be deallocated"):
                node.refresh(ctx)
                if node.request_id is None:
                    break
        self.assertTrue(all(n.state == n.State.READY for n in nodes))

        self.log.debug("Starting third request")
        request3 = self.requestNodes(["debian-normal", "debian-normal"])
        nodes3 = self.getNodes(request3)
        self.assertEqual(2, len(nodes3))
        self.assertEqual(nodes3[0].provider, nodes3[1].provider)
        ready_nodes_uuids = set(x.uuid for x in (nodes1 + nodes2))
        nodes3_uuids = set(x.uuid for x in nodes3)
        self.assertTrue(ready_nodes_uuids.intersection(nodes3_uuids))


class TestLauncherUpload(LauncherBaseTestCase):

    def setUp(self):
        self._ubuntu_images = []
        self.useFixture(fixtures.MonkeyPatch(
            'zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
            self._upload_run))
        super().setUp()

    def _waitForUploads(self, image_name, count,
                        states=(model.ImageUpload.State.FAILED,
                                model.ImageUpload.State.READY)):
        # This method is unique for this test, checking for final
        # states.
        for _ in iterate_timeout(30, "uploads to settle"):
            uploads = self.launcher.image_upload_registry.\
                getUploadsForImage(image_name)
            uploads = [u for u in uploads
                       if u.state in states]
            if len(uploads) == count:
                return uploads

    def _upload_run(test, self, *args, **kw):
        # Fail on first upload of this image
        if 'ubuntu-local' in self.image_name:
            upload_id = self.metadata['zuul_upload_uuid']
            upload = test.launcher.image_upload_registry.getItem(upload_id)
            artifact_id = upload.artifact_uuid
            if artifact_id not in test._ubuntu_images:
                test._ubuntu_images.append(artifact_id)
            if test._ubuntu_images[0] == artifact_id:
                test._addFinishedUpload(upload_id)
                raise Exception("Upload test failure")
        return "test_external_id"

    @simple_layout('layouts/nodepool-image.yaml', enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.debian_return_data,
    )
    @return_data(
        'build-ubuntu-local-image',
        'refs/heads/master',
        LauncherBaseTestCase.ubuntu_return_data,
    )
    @okay_tracebacks('Upload test failure')
    def test_launcher_image_expire_failed_upload(self):
        # This tests that we correctly expire the uploads and artifact
        # for an image artifact with no successful uploads.
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local', 'ubuntu-local'],
        )
        self.waitUntilSettled("start")
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))

        for _ in iterate_timeout(
                30, "scheduler and launcher to have the same layout"):
            if (self.scheds.first.sched.local_layout_state.get("tenant-one") ==
                self.launcher.local_layout_state.get("tenant-one")):
                break

        # The build should not run again because the image is no
        # longer missing
        self.waitUntilSettled("reconfigure")
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)

        for name in [
                'review.example.com%2Forg%2Fcommon-config/debian-local',
                'review.example.com%2Forg%2Fcommon-config/ubuntu-local',
        ]:
            artifacts = self.waitForArtifacts(name, 1, format='raw')
            self.waitForArtifacts(name, 0, format='qcow2')
            self.assertEqual('raw', artifacts[0].format)
            self.assertTrue(artifacts[0].validated)
            uploads = self._waitForUploads(name, 1)
            self.assertEqual(artifacts[0].uuid, uploads[0].artifact_uuid)
            if 'ubuntu' in name:
                self.assertIsNone(uploads[0].external_id)
                self.assertEqual('failed', uploads[0].state)
            else:
                self.assertEqual("test_external_id", uploads[0].external_id)
                self.assertEqual('ready', uploads[0].state)
            self.assertTrue(uploads[0].validated)

        # This is for ubuntu-local
        oldest_artifact_uuid = artifacts[0].uuid
        # At this point, we have:
        # debian-local: 1 artifact, 1 ready upload
        # ubuntu-local: 1 artifact, 1 failed upload
        # Both are raw artifacts. The qcow2 artifacts will have been
        # created and deleted since they have no uploads.

        # Run another build event manually
        image_cname = 'review.example.com%2Forg%2Fcommon-config/ubuntu-local'
        driver = self.launcher.connections.drivers['zuul']
        event = driver.getImageBuildEvent(
            ['ubuntu-local'], 'review.example.com',
            'org/common-config', 'master')
        self.launcher.trigger_events['tenant-one'].put(
            event.trigger_name, event)
        self.waitUntilSettled("manual build")
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)
        artifacts = self.waitForArtifacts(image_cname, 1, format='raw')
        uploads = self._waitForUploads(
            image_cname, 1,
            states=(model.ImageUpload.State.READY,))
        # delete the old failed upload now that it's older than a
        # ready upload.
        # At this point, we have:
        # debian-local: 1 artifact, 1 ready upload
        # ubuntu-local: 1 artifacts, 1 ready upload
        self.assertEqual('ready', uploads[0].state)

        # Run another build event manually
        driver = self.launcher.connections.drivers['zuul']
        event = driver.getImageBuildEvent(
            ['ubuntu-local'], 'review.example.com',
            'org/common-config', 'master')
        self.launcher.trigger_events['tenant-one'].put(
            event.trigger_name, event)
        self.waitUntilSettled("second manual build")
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
            dict(name='build-ubuntu-local-image', result='SUCCESS'),
        ], ordered=False)
        # Trigger a run of the deletion check.
        self.launcher.upload_deleted_event.set()
        self.launcher.wake_event.set()
        self.waitUntilSettled("deletion check")
        # At this point, we have:
        # debian-local: 1 artifact, 1 ready upload
        # ubuntu-local: 2 artifacts, 2 ready uploads
        artifacts = self.waitForArtifacts(image_cname, 2, format='raw')
        artifact_uuids = [x.uuid for x in artifacts]
        self.assertNotIn(oldest_artifact_uuid, artifact_uuids)
        uploads = self._waitForUploads(image_cname, 2)
        self.assertEqual('ready', uploads[0].state)
        self.assertEqual('ready', uploads[1].state)


class TestMinReadyLauncher(LauncherBaseTestCase):
    tenant_config_file = "config/launcher-min-ready/main.yaml"

    def test_min_ready(self):
        # tenant-one:
        #   common-config:
        #     Provider aws-us-east-1-main
        #       debian-normal (t3.medium)  (hash A)
        #   project1:
        #     Provider aws-ca-central-1-main
        #       debian-normal (t3.small)   (hash B)
        # tenant-two:
        #   common-config:
        #     Provider aws-us-east-1-main
        #       debian-normal (t3.medium)  (hash A)

        # min-ready=2 for debian-normal
        #   2 from aws-us-east-1-main
        #   0-2 from aws-ca-central-1-main
        # min-ready=1 for debian-emea
        #   1 from aws-eu-central-1-main
        for _ in iterate_timeout(60, "nodes to be ready"):
            nodes = self.launcher.api.nodes_cache.getItems()
            # Since we are randomly picking a provider to fill the
            # min-ready slots we might end up with 3-5 nodes
            # depending on the choice of providers.
            if not 3 <= len(nodes) <= 5:
                continue
            if all(n.state == n.State.READY for n in nodes):
                break

        self.waitUntilSettled()
        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertGreaterEqual(len(nodes), 3)
        self.assertLessEqual(len(nodes), 5)

        # Make sure the ready nodes show up in stats, even though they
        # are not on a provider.
        self.launcher._runStats()
        pname = 'review_example_com%2Fcommon-config_aws-us-east-1-main'
        self.assertReportedStat(
            f'zuul.provider.{pname}.label.debian-normal.nodes.state.ready',
            value='2',
            kind='g')
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        for _ in iterate_timeout(30, "nodes to be in-use"):
            # We expect the launcher to use the min-ready nodes
            # project2 will definitely use a ready node; project1 may
            # or may not depending on which providers were selected
            # for ready nodes and nodeset requests.
            in_use_nodes = [n for n in nodes if n.state == n.State.IN_USE]
            if len(in_use_nodes) >= 1:
                break

        self.assertEqual(nodes[0].host_keys, [])

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        check_job_a = self.getJobFromHistory('check-job', project=A.project)
        self.assertEqual(check_job_a.result,
                         'SUCCESS')
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2)
        self.assertEqual(check_job_a.node,
                         'debian-normal')

        check_job_b = self.getJobFromHistory('check-job', project=A.project)
        self.assertEqual(check_job_b.result,
                         'SUCCESS')
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2)
        self.assertEqual(check_job_b.node,
                         'debian-normal')

        # Wait for min-ready slots to be refilled
        for _ in iterate_timeout(60, "nodes to be ready"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if not 3 <= len(nodes) <= 5:
                continue
            if all(n.state == n.State.READY for n in nodes):
                break

        self.waitUntilSettled()
        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertGreaterEqual(len(nodes), 3)
        self.assertLessEqual(len(nodes), 5)

    def test_max_ready_age(self):
        for _ in iterate_timeout(60, "nodes to be ready"):
            nodes = self.launcher.api.nodes_cache.getItems()
            # Since we are randomly picking a provider to fill the
            # min-ready slots we might end up with 3-5 nodes
            # depending on the choice of providers.
            if not 3 <= len(nodes) <= 5:
                continue
            if all(n.state == n.State.READY for n in nodes):
                break

        self.waitUntilSettled('start')
        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertGreaterEqual(len(nodes), 3)
        self.assertLessEqual(len(nodes), 5)

        nodes_by_label = self._nodes_by_label()
        self.assertEqual(1, len(nodes_by_label['debian-emea']))
        node = nodes_by_label['debian-emea'][0]
        ctx = self.createZKContext(None)
        # Make a new copy so we can obtain our own lock
        node = model.ProviderNode.fromZK(ctx, path=node.getPath())
        try:
            node.acquireLock(ctx)
            node.updateAttributes(ctx, state_time=0)
        finally:
            node.releaseLock(ctx)

        for _ in iterate_timeout(60, "node to be deleted"):
            try:
                node.refresh(ctx)
            except NoNodeError:
                break

        self.waitUntilSettled('deleted')
        for _ in iterate_timeout(60, "node to be replaced"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if node in nodes:
                continue
            if not 3 <= len(nodes) <= 5:
                continue
            if all(n.state == n.State.READY for n in nodes):
                break

        self.waitUntilSettled('replaced')
        nodes_by_label = self._nodes_by_label()
        self.assertEqual(1, len(nodes_by_label['debian-emea']))

    def test_max_age(self):
        for _ in iterate_timeout(60, "nodes to be ready"):
            nodes = self.launcher.api.nodes_cache.getItems()
            # Since we are randomly picking a provider to fill the
            # min-ready slots we might end up with 3-5 nodes
            # depending on the choice of providers.
            if not 3 <= len(nodes) <= 5:
                continue
            if all(n.state == n.State.READY for n in nodes):
                break

        self.waitUntilSettled('start')
        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertGreaterEqual(len(nodes), 3)
        self.assertLessEqual(len(nodes), 5)

        nodes_by_label = self._nodes_by_label()
        self.assertEqual(1, len(nodes_by_label['debian-emea']))
        node = nodes_by_label['debian-emea'][0]
        ctx = self.createZKContext(None)
        # Make a new copy so we can obtain our own lock
        node = model.ProviderNode.fromZK(ctx, path=node.getPath())
        try:
            node.acquireLock(ctx)
            node.updateAttributes(ctx, request_time=0)
        finally:
            node.releaseLock(ctx)

        for _ in iterate_timeout(60, "node to be deleted"):
            try:
                node.refresh(ctx)
            except NoNodeError:
                break

        self.waitUntilSettled('deleted')
        for _ in iterate_timeout(60, "node to be replaced"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if node in nodes:
                continue
            if not 3 <= len(nodes) <= 5:
                continue
            if all(n.state == n.State.READY for n in nodes):
                break

        self.waitUntilSettled('replaced')
        nodes_by_label = self._nodes_by_label()
        self.assertEqual(1, len(nodes_by_label['debian-emea']))


class TestMinReadyTenantVariant(LauncherBaseTestCase):
    tenant_config_file = "config/launcher-min-ready/tenant-variant.yaml"

    def test_min_ready(self):
        # tenant-one:
        #   common-config:
        #     Provider aws-us-east-1-main
        #       debian-normal (t3.medium)  (hash A1)
        #   project1:
        #     Provider aws-eu-central-1-main
        #       debian-emea
        #     Provider aws-ca-central-1-main
        #       debian-normal (t3.small)   (hash B1)
        # tenant-two:
        #   common-config:
        #     Provider aws-us-east-1-main
        #       debian-normal (t3.medium)  (hash C)
        #   project2:
        #     Image debian (for tenant 2)

        # min-ready=2 for debian-normal
        #   2 from aws-us-east-1-main
        #   2 from aws-ca-central-1-main
        # min-ready=1 for debian-emea
        #   1 from aws-eu-central-1-main
        for _ in iterate_timeout(60, "nodes to be ready"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if len(nodes) != 5:
                continue
            if all(n.state == n.State.READY for n in nodes):
                break

        self.waitUntilSettled()
        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(5, len(nodes))

        nodes_by_label = self._nodes_by_label()
        self.assertEqual(4, len(nodes_by_label['debian-normal']))
        debian_normal_cfg_hashes = {
            n.label_config_hash for n in nodes_by_label['debian-normal']
        }
        # We will get 2 nodes with hash C, and then 2 nodes with hash
        # A1 or B1, so that's 2 or 3 hashes.
        self.assertGreaterEqual(len(debian_normal_cfg_hashes), 2)
        self.assertLessEqual(len(debian_normal_cfg_hashes), 3)

        files = {
            'zuul-extra.d/image.yaml': textwrap.dedent(
                '''
                - image:
                    name: debian
                    type: cloud
                    description: "Debian test image"
                '''
            )
        }
        # This will create hashes A2 and B2
        self.addCommitToRepo('org/project1', 'Change label config', files)
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        for _ in iterate_timeout(60, "nodes to be ready"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if len(nodes) != 8:
                continue
            if all(n.state == n.State.READY for n in nodes):
                break

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(8, len(nodes))

        nodes_by_label = self._nodes_by_label()
        self.assertEqual(2, len(nodes_by_label['debian-emea']))
        self.assertEqual(6, len(nodes_by_label['debian-normal']))
        debian_normal_cfg_hashes = {
            n.label_config_hash for n in nodes_by_label['debian-normal']
        }
        # Hash C, and any combination of A1, A2, B1, and B2:
        # Least case example: 2xC, 4xA1
        # Most case example: 2xC, 1xA1, 1xA2, 1xB1, 1xB2
        self.assertGreaterEqual(len(debian_normal_cfg_hashes), 2)
        self.assertLessEqual(len(debian_normal_cfg_hashes), 5)


class TestNodesetRequestPriority(LauncherBaseTestCase):
    config_file = 'zuul.conf'
    tenant_config_file = 'config/single-tenant/main-launcher.yaml'

    def test_pipeline_priority(self):
        "Test that nodes are requested at the correct pipeline priority"
        self.hold_nodeset_requests_in_queue = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.setMerged()
        self.fake_gerrit.addEvent(A.getRefUpdatedEvent())
        self.waitUntilSettled()

        B = self.fake_gerrit.addFakeChange('org/project1', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        C.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))
        self.waitUntilSettled()

        reqs = self.launcher.api.getNodesetRequests()

        # The requests come back sorted by priority. Since we have
        # three requests for the three changes each with a different
        # priority.

        # * gate first - high priority - change C
        self.assertEqual(reqs[0].priority, 100)
        self.assertEqual(reqs[0].labels, ['label1'])
        # * check second - normal priority - change B
        self.assertEqual(reqs[1].priority, 200)
        self.assertEqual(reqs[1].labels, ['label1'])
        # * post third - low priority - change A
        # additionally, the post job defined uses an ubuntu-xenial node,
        # so we include that check just as an extra verification
        self.assertEqual(reqs[2].priority, 300)
        self.assertEqual(reqs[2].labels, ['ubuntu-xenial'])

        self.hold_nodeset_requests_in_queue = False
        self.releaseNodesetRequests(*reqs)

        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='2,1'),
            dict(name='project-test1', result='SUCCESS', changes='2,1'),
            dict(name='project-test2', result='SUCCESS', changes='2,1'),
            dict(name='project-merge', result='SUCCESS', changes='3,1'),
            dict(name='project-test1', result='SUCCESS', changes='3,1'),
            dict(name='project-test2', result='SUCCESS', changes='3,1'),
            dict(name='project1-project2-integration',
                 result='SUCCESS', changes='2,1'),
            dict(name='project-post', result='SUCCESS'),
        ], ordered=False)

    @simple_layout('layouts/two-projects-integrated-launcher.yaml',
                   enable_nodepool=True)
    def test_relative_priority_check(self):
        "Test that nodes are requested at the relative priority"
        self.hold_nodeset_requests_in_queue = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        C = self.fake_gerrit.addFakeChange('org/project1', 'master', 'C')
        self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        D = self.fake_gerrit.addFakeChange('org/project2', 'master', 'D')
        self.fake_gerrit.addEvent(D.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # The requests come back sorted by priority.
        reqs = self.launcher.api.getNodesetRequests()

        # Change A, first change for project, high relative priority.
        self.assertEqual(reqs[0].priority, 200)
        self.assertEqual(reqs[0].relative_priority, 0)

        # Change C, first change for project1, high relative priority.
        self.assertEqual(reqs[1].priority, 200)
        self.assertEqual(reqs[1].relative_priority, 0)

        # Change B, second change for project, lower relative priority.
        self.assertEqual(reqs[2].priority, 200)
        self.assertEqual(reqs[2].relative_priority, 1)

        # Change D, first change for project2 shared with project1,
        # lower relative priority than project1.
        self.assertEqual(reqs[3].priority, 200)
        self.assertEqual(reqs[3].relative_priority, 1)

        # Fulfill only the first request
        ctx = self.createZKContext(None)
        reqs[0].updateAttributes(ctx, state=reqs[0].State.REQUESTED)
        for x in iterate_timeout(30, 'fulfill request'):
            reqs = self.launcher.api.getNodesetRequests()
            if len(reqs) < 4:
                break
        self.waitUntilSettled()

        reqs = self.launcher.api.getNodesetRequests()

        # Change B, now first change for project, equal priority.
        self.assertEqual(reqs[0].priority, 200)
        self.assertEqual(reqs[0].relative_priority, 0)

        # Change C, now first change for project1, equal priority.
        self.assertEqual(reqs[1].priority, 200)
        self.assertEqual(reqs[1].relative_priority, 0)

        # Change D, first change for project2 shared with project1,
        # still lower relative priority than project1.
        self.assertEqual(reqs[2].priority, 200)
        self.assertEqual(reqs[2].relative_priority, 1)

        self.hold_nodeset_requests_in_queue = False
        self.releaseNodesetRequests(*reqs)
        self.waitUntilSettled()

    @simple_layout('layouts/two-projects-integrated-launcher.yaml',
                   enable_nodepool=True)
    def test_relative_priority_long(self):
        "Test that nodes are requested at the relative priority"
        self.hold_nodeset_requests_in_queue = True

        count = 13
        changes = []
        for x in range(count):
            change = self.fake_gerrit.addFakeChange(
                'org/project', 'master', 'A')
            self.fake_gerrit.addEvent(change.getPatchsetCreatedEvent(1))
            self.waitUntilSettled()
            changes.append(change)

        reqs = self.launcher.api.getNodesetRequests()
        self.assertEqual(len(reqs), 13)
        # The requests come back sorted by priority.
        for x in range(10):
            self.assertEqual(reqs[x].relative_priority, x)
        self.assertEqual(reqs[10].relative_priority, 10)
        self.assertEqual(reqs[11].relative_priority, 10)
        self.assertEqual(reqs[12].relative_priority, 10)

        # Fulfill only the first request
        self.releaseNodesetRequests(reqs[0])
        self.waitUntilSettled()

        reqs = self.launcher.api.getNodesetRequests()
        self.assertEqual(len(reqs), 12)
        for x in range(10):
            self.assertEqual(reqs[x].relative_priority, x)
        self.assertEqual(reqs[10].relative_priority, 10)
        self.assertEqual(reqs[11].relative_priority, 10)

        self.hold_nodeset_requests_in_queue = False
        self.releaseNodesetRequests(*reqs)
        self.waitUntilSettled()

    @simple_layout('layouts/two-projects-integrated-launcher.yaml',
                   enable_nodepool=True)
    def test_relative_priority_gate(self):
        "Test that nodes are requested at the relative priority"
        self.hold_nodeset_requests_in_queue = True

        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled()

        # project does not share a queue with project1 and project2.
        C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
        C.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(C.addApproval('Approved', 1))
        self.waitUntilSettled()

        # The requests come back sorted by priority.
        reqs = self.launcher.api.getNodesetRequests()

        # Change A, first change for shared queue, high relative
        # priority.
        self.assertEqual(reqs[0].priority, 100)
        self.assertEqual(reqs[0].relative_priority, 0)

        # Change C, first change for independent project, high
        # relative priority.
        self.assertEqual(reqs[1].priority, 100)
        self.assertEqual(reqs[1].relative_priority, 0)

        # Change B, second change for shared queue, lower relative
        # priority.
        self.assertEqual(reqs[2].priority, 100)
        self.assertEqual(reqs[2].relative_priority, 1)

        self.hold_nodeset_requests_in_queue = False
        self.releaseNodesetRequests(*reqs)
        self.waitUntilSettled()


class TestExecutorZone(LauncherBaseTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'

    def setup_config(self, config_file: str):
        config = super().setup_config(config_file)
        config.set('executor', 'zone', 'us-east-1')
        return config

    @simple_layout('layouts/nodepool-executor-zone.yaml', enable_nodepool=True)
    def test_jobs_executed(self):
        self.hold_jobs_in_queue = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        queue = list(self.executor_api.queued())
        self.assertEqual(len(self.builds), 0)
        self.assertEqual(len(queue), 1)
        self.assertEqual('us-east-1', queue[0].zone)

        self.hold_jobs_in_queue = False
        self.executor_api.release()
        self.waitUntilSettled()

        self.assertEqual(self.getJobFromHistory('check-job').result,
                         'SUCCESS')
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2)
        self.assertEqual(self.getJobFromHistory('check-job').node,
                         'debian-normal')


class TestNodepool(LauncherBaseTestCase):
    # Test nodepool-in-zuul features that require a full config.
    tenant_config_file = 'config/nodepool/main.yaml'

    def test_normal_label(self):
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='debian-normal-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    def test_unattached_label(self):
        in_repo_conf = textwrap.dedent(
            """
            - project:
                check:
                  jobs:
                    - debian-unattached-job
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1)
        self.assertTrue(re.search('debian-unattached-job .* NODE_FAILURE',
                                  A.messages[0]))

    def test_missing_label(self):
        in_repo_conf = textwrap.dedent(
            """
            - job:
                name: debian-missing-job
                nodeset:
                  nodes:
                    - label: debian-missing
                      name: controller
            - project:
                check:
                  jobs:
                    - debian-missing-job
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.patchsets[0]['approvals'][0]['value'], "-1")
        self.assertIn('The label "debian-missing" was not found',
                      A.messages[0],
                      "A should have failed the check pipeline")

    @return_data(
        'build-debian-new-image',
        'refs/changes/01/1/1',
        LauncherBaseTestCase.debian_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="ami-1e749f67")
    def test_image_addition(self, mock_uploadimage):
        # Test that we can add a new image and the jobs to build it in
        # a single change.
        in_repo_conf = textwrap.dedent(
            """
            - image:
                name: debian-new
                type: zuul
            - job:
                name: build-debian-new-image
                image-build-name: debian-new
            - project:
                gate:
                  jobs:
                    - build-debian-new-image
            - provider:
                name: aws-new
                section: aws-us-east-1
                images:
                  - name: debian-new
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertHistory([
            dict(name='build-debian-new-image',
                 result='SUCCESS', changes='1,1'),
        ], ordered=False)
        # The provider should not be known to the launcher yet.
        self.assertNotIn('aws-new',
                         self.launcher.tenant_providers['tenant-one'])
        # Perform the reconfiguration after the change merges.
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()
        provider = self.launcher._getProvider(
            'tenant-one', 'aws-new')
        self.assertIn('debian-new', provider.images)
        # We should have two artifacts for it.
        image_cname = 'review.example.com%2Forg%2Fproject1/debian-new'
        self.waitForArtifacts(image_cname, 2)

    @return_data(
        'build-debian-new-image',
        'refs/changes/01/1/1',
        LauncherBaseTestCase.debian_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="ami-1e749f67")
    def test_label_addition(self, mock_uploadimage):
        # Building on the previous test, verify that we can add a
        # label to a new provider.
        in_repo_conf = textwrap.dedent(
            """
            - image:
                name: debian-new
                type: zuul
            - label:
                name: debian-new
                image: debian-new
                flavor: normal
            - job:
                name: build-debian-new-image
                image-build-name: debian-new
            - job:
                name: testjob
                nodeset:
                  nodes:
                    - label: debian-new
                      name: controller
            - project:
                gate:
                  jobs:
                    - build-debian-new-image
            - provider:
                name: aws-new
                section: aws-us-east-1
                images:
                  - name: debian-new
                labels:
                  - name: debian-new
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertHistory([
            dict(name='build-debian-new-image',
                 result='SUCCESS', changes='1,1'),
        ], ordered=False)
        # The provider should not be known to the launcher yet.
        self.assertNotIn('aws-new',
                         self.launcher.tenant_providers['tenant-one'])
        # Perform the reconfiguration after the change merges.
        self.fake_gerrit.addEvent(A.getChangeMergedEvent())
        self.waitUntilSettled()
        provider = self.launcher._getProvider(
            'tenant-one', 'aws-new')
        self.assertIn('debian-new', provider.images)
        self.assertIn('debian-new', provider.labels)
        # We should have two artifacts for it.
        image_cname = 'review.example.com%2Forg%2Fproject1/debian-new'
        self.waitForArtifacts(image_cname, 2)

    @return_data(
        'build-debian-new-image',
        'refs/changes/01/1/1',
        LauncherBaseTestCase.debian_return_data,
    )
    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageUploadJob.run',
                return_value="ami-1e749f67")
    def test_label_addition_limits(self, mock_uploadimage):
        # Building on the previous test, verify that we can't actually
        # use a new label that we dynamically add.
        in_repo_conf = textwrap.dedent(
            """
            - image:
                name: debian-new
                type: zuul
            - label:
                name: debian-new
                image: debian-new
                flavor: normal
            - job:
                name: build-debian-new-image
                image-build-name: debian-new
            - job:
                name: testjob
                nodeset:
                  nodes:
                    - label: debian-new
                      name: controller
            - project:
                check:
                  jobs:
                    - testjob
                gate:
                  jobs:
                    - testjob
                    - build-debian-new-image
            - provider:
                name: aws-new
                section: aws-us-east-1
                images:
                  - name: debian-new
                labels:
                  - name: debian-new
            """)
        file_dict = {'zuul.yaml': in_repo_conf}
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A',
                                           files=file_dict)
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()
        self.assertEqual(A.data['status'], 'NEW')
        self.assertHistory([
            dict(name='build-debian-new-image',
                 result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertIn('NODE_FAILURE', A.messages[1])


class TestSnapshot(AnsibleZuulTestCase, LauncherBaseTestCase):
    tenant_config_file = 'config/snapshot/main.yaml'

    def lateSetUp(self):
        orig_getInstanceConfiguration = zuul.driver.aws.awsendpoint.\
            AwsProviderEndpoint._getInstanceConfiguration

        def getInstanceConfiguration(self, *args, **kw):
            args = orig_getInstanceConfiguration(self, *args, **kw)
            args['NetworkInterfaces'][0]['PrivateIpAddress'] = '127.0.0.1'
            return args
        self.patch(zuul.driver.aws.awsendpoint.AwsProviderEndpoint,
                   '_getInstanceConfiguration',
                   getInstanceConfiguration)

        def advance(self):
            self.complete = True
            # This isn't what the URL will really look like, but it's
            # enough to trigger an import which is what we will
            # eventually want and have stubbed out in the test below.
            return "s3://zuul/doesnotexist"
        self.patch(zuul.driver.aws.awsendpoint.AwsSnapshotStateMachine,
                   'advance',
                   advance)

        orig_updateNode = zuul.driver.aws.awsendpoint.AwsCreateStateMachine.\
            _updateNode

        def _updateNode(self):
            orig_updateNode(self)
            self.node.connection_type = 'local'
        self.patch(zuul.driver.aws.awsendpoint.AwsCreateStateMachine,
                   '_updateNode',
                   _updateNode)

    def _waitForUploads(self, image_cname, count=None):
        for _ in iterate_timeout(60, "upload to complete"):
            uploads = self.launcher.image_upload_registry.getUploadsForImage(
                image_cname)
            pending = [u for u in uploads if u.external_id is None]
            if not pending:
                if count is None or count == len(uploads):
                    return uploads

    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageImportJob.run',
                return_value="test_external_id")
    def test_snapshot_e2e(self, import_mock):
        self.executor_server.keep_jobdir = True
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/common-config',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled()
        image_cname = 'review.example.com%2Fcommon-config/debian-local'
        # We have an image, that's good enough for this test.
        self._waitForUploads(image_cname, 1)
        # Check the result from the DB since snapshot failures
        # override the playbook result.
        connection = self.scheds.first.sched.sql.connection
        builds = connection.getBuilds()
        self.assertEqual(1, len(builds))
        self.assertEqual('SUCCESS', builds[0].result)

        build = self.getJobFromHistory(
            'build-debian-local-image', result='SUCCESS')
        with open(build.jobdir.job_output_file) as f:
            output = f.read()
            self.log.debug(output)
        self.assertIn('Waiting for snapshots', output)

        sql = self.scheds.first.connections.getSqlConnection()
        with sql.getSession() as db:
            build = list(reversed(db.getBuilds()))[0]
            self.assertEqual(len(build.build_events), 2)
            self.assertEqual(
                build.build_events[0].event_type, "snapshot started")
            self.assertEqual(
                build.build_events[1].event_type, "snapshot completed")

    @mock.patch('zuul.driver.aws.awsendpoint.AwsImageImportJob.run',
                return_value="test_external_id")
    @okay_tracebacks('_checkNodeSnapshot')
    def test_snapshot_failure_e2e(self, import_mock):
        self.commitConfigUpdate(
            "common-config",
            'config/snapshot/git/common-config/timeout.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        def advance(self):
            self.state = "not start"
        self.patch(zuul.driver.aws.awsendpoint.AwsSnapshotStateMachine,
                   'advance',
                   advance)

        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/common-config',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled()
        connection = self.scheds.first.sched.sql.connection
        builds = connection.getBuilds()
        self.assertEqual(1, len(builds))
        self.assertEqual('SNAPSHOT_FAILURE', builds[0].result)


class TestSubnodesAndReuse(LauncherBaseTestCase):

    @simple_layout('layouts/nodepool-subnodes.yaml', enable_nodepool=True)
    def test_subnodes(self):
        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.State.FULFILLED, request.state)

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(3, len(nodes))
        # Get a list with the main node first and the subnodes last
        nodes.sort(key=lambda x: len(x.subnodes))
        nodes.reverse()
        main = nodes[0]
        sub1 = nodes[1]
        sub2 = nodes[2]
        # Get a copy so we're not modifying the launcher's
        main = model.ProviderNode.fromZK(ctx, path=main.getPath())
        sub1 = model.ProviderNode.fromZK(ctx, path=sub1.getPath())
        sub2 = model.ProviderNode.fromZK(ctx, path=sub2.getPath())
        self.assertIsNone(main.main_node_id)
        self.assertEqual(main.uuid, sub1.main_node_id)
        self.assertEqual(main.uuid, sub2.main_node_id)
        self.assertEqual(set([sub1.uuid, sub2.uuid]), set(main.subnodes))
        self.assertEqual([], sub1.subnodes)
        self.assertEqual([], sub2.subnodes)
        self.assertEqual(main.subnodes.index(sub1.uuid), sub1.slot)
        self.assertEqual(main.subnodes.index(sub2.uuid), sub2.slot)
        self.assertEqual(main.State.SLOT_HOST, main.state)
        self.assertEqual(sub1.State.READY, sub1.state)
        self.assertEqual(sub2.State.READY, sub2.state)
        if (sub1.request_id == request.uuid):
            self.assertIsNone(sub2.request_id)
        elif (sub2.request_id == request.uuid):
            self.assertIsNone(sub1.request_id)
        else:
            self.assertTrue(False, "one of the subnodes must be assigned")

        # Verify that the slot host is tagged correctly.
        system_id = self.launcher.system.system_id
        self.assertEqual(system_id, main.tags.get('zuul_system_id'))
        self.assertEqual(main.uuid, main.tags.get('zuul_node_uuid'))

        with sub1.locked(ctx):
            with sub1.activeContext(ctx):
                sub1.assign(ctx, request_id="dne", tenant_name="test")
                # Modify the request so that the max zxid of the
                # request cache is later than the node assignment
                # (since the "dne" request does not exist).
                with request.locked(ctx), request.activeContext(ctx):
                    request.priority += 1
                sub1.setState(sub1.State.USED)

        for _ in iterate_timeout(10, "sub1 to be deleted"):
            try:
                sub1.refresh(ctx)
            except NoNodeError:
                break

        # Main and sub2 should still exist
        main.refresh(ctx)
        sub2.refresh(ctx)

        with sub2.locked(ctx):
            with sub2.activeContext(ctx):
                sub2.unassign(ctx)
                sub2.assign(ctx, request_id="dne", tenant_name="test")
                # Modify the request so that the max zxid of the
                # request cache is later than the node assignment
                # (since the "dne" request does not exist).
                with request.locked(ctx), request.activeContext(ctx):
                    request.priority += 1
                sub2.setState(sub2.State.USED)

        for _ in iterate_timeout(10, "sub2 to be deleted"):
            try:
                sub2.refresh(ctx)
            except NoNodeError:
                break

        for _ in iterate_timeout(10, "main to be deleted"):
            try:
                main.refresh(ctx)
            except NoNodeError:
                break

    @simple_layout('layouts/nodepool-subnodes.yaml', enable_nodepool=True)
    @okay_tracebacks('_checkNodeCreate')
    def test_subnodes_crash_during_create(self):
        # Test that we clean up subnodes if a launcher crashes while
        # creating them after having created the main node.
        self.useFixture(fixtures.MonkeyPatch(
            'zuul.launcher.server.Launcher.SUBNODE_SYNC_TIME',
            2))

        event = threading.Event()
        orig_getProviderNode = zuul.zk.launcher.LauncherApi.getProviderNode

        def getProviderNode(*args, **kw):
            if event.is_set():
                return orig_getProviderNode(*args, **kw)

        self.useFixture(fixtures.MonkeyPatch(
            'zuul.zk.launcher.LauncherApi.getProviderNode',
            getProviderNode))

        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'], timeout=None)

        for _ in iterate_timeout(10, "main to fail"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if len(nodes) != 3:
                continue
            # Get a list with the main node first and the subnodes last
            nodes.sort(key=lambda x: len(x.subnodes))
            nodes.reverse()
            main = nodes[0]
            if main.state == main.State.FAILED:
                break
        event.set()

        # The rest is just cleanup; the request will be retried and
        # fulfilled.
        for _ in iterate_timeout(60, "request to be fulfilled"):
            request.refresh(ctx)
            if request.state == model.NodesetRequest.State.FULFILLED:
                break

    @simple_layout('layouts/nodepool-subnodes-ready.yaml',
                   enable_nodepool=True)
    def test_subnodes_max_ready_age(self):
        for _ in iterate_timeout(60, "nodes to be ready"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if any(n.state == n.State.SLOT_HOST for n in nodes):
                break

        self.waitUntilSettled('start')
        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(3, len(nodes))
        # Get a list with the main node first and the subnode last
        nodes.sort(key=lambda x: len(x.subnodes))
        nodes.reverse()
        main = nodes[0]
        sub1 = nodes[1]
        sub2 = nodes[2]
        ctx = self.createZKContext(None)

        sub1 = model.ProviderNode.fromZK(ctx, path=sub1.getPath())
        with sub1.locked(ctx):
            sub1.updateAttributes(ctx, state_time=0)

        for _ in iterate_timeout(10, "sub1 node to be deleted"):
            try:
                sub1.refresh(ctx)
            except NoNodeError:
                break

        sub2 = model.ProviderNode.fromZK(ctx, path=sub2.getPath())
        with sub2.locked(ctx):
            sub2.updateAttributes(ctx, state_time=0)

        for _ in iterate_timeout(10, "sub2 node to be deleted"):
            try:
                sub2.refresh(ctx)
            except NoNodeError:
                break

        for _ in iterate_timeout(10, "main node to be deleted"):
            try:
                main.refresh(ctx)
            except NoNodeError:
                break

        self.waitUntilSettled('deleted')
        for _ in iterate_timeout(10, "node to be replaced"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if any(n.state == n.State.SLOT_HOST for n in nodes):
                break

    @simple_layout('layouts/nodepool-subnodes-ready.yaml',
                   enable_nodepool=True)
    def test_subnodes_max_age(self):
        for _ in iterate_timeout(60, "nodes to be ready"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if any(n.state == n.State.SLOT_HOST for n in nodes):
                break

        self.waitUntilSettled('start')
        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(3, len(nodes))
        # Get a list with the main node first and the subnode last
        nodes.sort(key=lambda x: len(x.subnodes))
        nodes.reverse()
        main = nodes[0]
        sub1 = nodes[1]
        sub2 = nodes[2]
        ctx = self.createZKContext(None)

        sub1 = model.ProviderNode.fromZK(ctx, path=sub1.getPath())
        with sub1.locked(ctx):
            sub1.updateAttributes(ctx, request_time=0)

        for _ in iterate_timeout(10, "sub1 node to be deleted"):
            try:
                sub1.refresh(ctx)
            except NoNodeError:
                break

        sub2 = model.ProviderNode.fromZK(ctx, path=sub2.getPath())
        with sub2.locked(ctx):
            sub2.updateAttributes(ctx, request_time=0)

        for _ in iterate_timeout(10, "sub2 node to be deleted"):
            try:
                sub2.refresh(ctx)
            except NoNodeError:
                break

        for _ in iterate_timeout(10, "main node to be deleted"):
            try:
                main.refresh(ctx)
            except NoNodeError:
                break

        self.waitUntilSettled('deleted')
        for _ in iterate_timeout(10, "node to be replaced"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if any(n.state == n.State.SLOT_HOST for n in nodes):
                break

    @simple_layout('layouts/nodepool-reuse.yaml', enable_nodepool=True)
    def test_reuse(self):
        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.State.FULFILLED, request.state)

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(1, len(nodes))
        node = nodes[0]
        # Get a copy so we're not modifying the launcher's
        node = model.ProviderNode.fromZK(ctx, path=node.getPath())

        with node.locked(ctx):
            with node.activeContext(ctx):
                node.unassign(ctx)
                node.assign(ctx, request_id="dne", tenant_name="test")
                # Modify the request so that the max zxid of the
                # request cache is later than the node assignment
                # (since the "dne" request does not exist).
                with request.locked(ctx), request.activeContext(ctx):
                    request.priority += 1
                node.setState(node.State.USED)

        for _ in iterate_timeout(10, "node to be recycled"):
            node.refresh(ctx)
            if node.state == node.State.READY:
                break

        # Max-age should cause it to be deleted
        with node.locked(ctx):
            node.updateAttributes(ctx, request_time=0)

        for _ in iterate_timeout(10, "node to be deleted"):
            try:
                node.refresh(ctx)
            except NoNodeError:
                break

    @simple_layout('layouts/nodepool-reuse.yaml', enable_nodepool=True)
    @okay_tracebacks('_checkNodescanRequest')
    def test_reuse_unreachable(self):
        # Regression test to validate a node can be reused
        # multiple times, while performing a nodescan each time.
        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.State.FULFILLED, request.state)

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(1, len(nodes))
        node = nodes[0]
        # Get a copy so we're not modifying the launcher's
        node = model.ProviderNode.fromZK(ctx, path=node.getPath())

        with node.locked(ctx):
            with node.activeContext(ctx):
                node.unassign(ctx)
                node.assign(ctx, request_id="dne", tenant_name="test")
                # Modify the request so that the max zxid of the
                # request cache is later than the node assignment
                # (since the "dne" request does not exist).
                with request.locked(ctx), request.activeContext(ctx):
                    request.priority += 1
                node.setState(node.State.USED)

        for _ in iterate_timeout(10, "node to be recycled"):
            node.refresh(ctx)
            if node.state == node.State.READY:
                break

        def my_advance(*args, **kw):
            raise Exception("Test exception")

        # Fail the nodescan and check that the node is correctly
        # marked as failed and NOT reused.
        with mock.patch.object(
            zuul.launcher.server.NodescanRequest, 'advance', my_advance
        ):
            with node.locked(ctx):
                with node.activeContext(ctx):
                    node.assign(ctx, request_id="dne", tenant_name="test")
                    with request.locked(ctx), request.activeContext(ctx):
                        request.priority += 1
                    node.setState(node.State.USED)

            for _ in iterate_timeout(10, "node to be failed"):
                node.refresh(ctx)
                if node.state == node.State.FAILED:
                    break

        self.assertEqual(node.State.FAILED, node.state)

    @simple_layout('layouts/nodepool-reuse.yaml', enable_nodepool=True)
    def test_min_retention_time(self):
        ctx = self.createZKContext(None)
        request = self.requestNodes(['macos14-m2'])
        self.assertEqual(request.State.FULFILLED, request.state)

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(1, len(nodes))
        node = nodes[0]
        # Get a copy so we're not modifying the launcher's
        node = model.ProviderNode.fromZK(ctx, path=node.getPath())

        with node.locked(ctx):
            with node.activeContext(ctx):
                node.unassign(ctx)
                node.assign(ctx, request_id="dne", tenant_name="test")
                # Modify the request so that the max zxid of the
                # request cache is later than the node assignment
                # (since the "dne" request does not exist).
                with request.locked(ctx), request.activeContext(ctx):
                    request.priority += 1
                # Node exceeding max-age should not impact the reuse.
                node.request_time -= 1200
                node.setState(node.State.USED)

        for _ in iterate_timeout(10, "node to be recycled"):
            node.refresh(ctx)
            if node.state == node.State.READY:
                break

        # Exceeding the min-retention time should cause it to be deleted,
        # since also max-age is exceeded.
        with node.locked(ctx):
            node.updateAttributes(
                ctx, request_time=node.request_time - 3600 * 24)

        for _ in iterate_timeout(10, "node to be deleted"):
            try:
                node.refresh(ctx)
            except NoNodeError:
                break

    @simple_layout('layouts/nodepool-subnodes-reuse.yaml',
                   enable_nodepool=True)
    def test_subnodes_reuse(self):
        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.State.FULFILLED, request.state)

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(3, len(nodes))
        # Get a list with the main node first and the subnode last
        nodes.sort(key=lambda x: len(x.subnodes))
        nodes.reverse()
        main = nodes[0]
        sub1 = nodes[1]
        sub2 = nodes[2]
        # Get a copy so we're not modifying the launcher's
        main = model.ProviderNode.fromZK(ctx, path=main.getPath())
        sub1 = model.ProviderNode.fromZK(ctx, path=sub1.getPath())
        sub2 = model.ProviderNode.fromZK(ctx, path=sub2.getPath())
        self.assertIsNone(main.main_node_id)
        self.assertEqual(main.uuid, sub1.main_node_id)
        self.assertEqual(main.uuid, sub2.main_node_id)
        self.assertEqual(set([sub1.uuid, sub2.uuid]), set(main.subnodes))
        self.assertEqual([], sub1.subnodes)
        self.assertEqual([], sub2.subnodes)
        self.assertEqual(main.State.SLOT_HOST, main.state)
        self.assertEqual(sub1.State.READY, sub1.state)
        self.assertEqual(sub2.State.READY, sub2.state)

        with sub1.locked(ctx):
            with sub1.activeContext(ctx):
                sub1.assign(ctx, request_id="dne", tenant_name="test")
                # Modify the request so that the max zxid of the
                # request cache is later than the node assignment
                # (since the "dne" request does not exist).
                with request.locked(ctx), request.activeContext(ctx):
                    request.priority += 1
                sub1.setState(sub1.State.USED)

        for _ in iterate_timeout(10, "sub to be recycled"):
            sub1.refresh(ctx)
            if sub1.state == sub1.State.READY:
                break
        self.assertEqual(sub1.uuid, nodes[1].uuid)

        # Main and sub2 should still exist
        main.refresh(ctx)
        sub2.refresh(ctx)

    @simple_layout('layouts/nodepool-subnodes.yaml', enable_nodepool=True)
    @okay_tracebacks('_checkNodescanRequest')
    def test_subnodes_failure(self):
        # Test a node failure
        ctx = self.createZKContext(None)

        def my_advance(*args, **kw):
            raise Exception("Test exception")

        with mock.patch.object(
            zuul.launcher.server.NodescanRequest, 'advance', my_advance
        ):
            request = self.requestNodes(["debian-normal"])

        # We retry; the first set of nodes will be deleted. Our stable
        # state is one main node and one subnode (the other is deleted
        # because it is unattached).
        for _ in iterate_timeout(10, "nodes to be deleted"):
            nodes = self.launcher.api.nodes_cache.getItems()
            states = Counter(n.state for n in nodes)
            if states['failed'] == 2:
                break

        self.assertEqual(request.state, model.NodesetRequest.State.FAILED)
        provider_node_data = request.provider_node_data[0]
        self.assertEqual(len(provider_node_data['failed_providers']), 1)
        self.assertEqual(len(request.nodes), 1)

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(2, len(nodes))
        # Get a list with the main node first and the subnode last
        nodes.sort(key=lambda x: len(x.subnodes))
        nodes.reverse()
        main = nodes[0]
        sub1 = nodes[1]
        # Get a copy so we're not modifying the launcher's
        main = model.ProviderNode.fromZK(ctx, path=main.getPath())
        sub1 = model.ProviderNode.fromZK(ctx, path=sub1.getPath())
        self.assertIsNone(main.main_node_id)
        self.assertEqual(main.uuid, sub1.main_node_id)
        # We don't know the expected uuid of sub2 because the test
        # never saw it, so we just assert the length and content of
        # sub1.
        self.assertEqual(2, len(main.subnodes))
        self.assertIn(sub1.uuid, main.subnodes)
        self.assertEqual([], sub1.subnodes)
        self.assertEqual(main.State.FAILED, main.state)
        self.assertEqual(sub1.State.FAILED, sub1.state)

        request.delete(ctx)
        self.waitUntilSettled()

        for _ in iterate_timeout(10, "nodes to be deleted"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if len(nodes) == 0:
                break

    @simple_layout('layouts/nodepool-subnodes-reuse.yaml',
                   enable_nodepool=True)
    @okay_tracebacks('_checkNodescanRequest')
    def test_subnodes_reuse_failure(self):
        # Test that if we detect a failure in a subnode after it is
        # used once that we mark it and the main node as failed for
        # later deletion.
        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.State.FULFILLED, request.state)

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(3, len(nodes))
        # Get a list with the main node first and the subnode last
        nodes.sort(key=lambda x: len(x.subnodes))
        nodes.reverse()
        main = nodes[0]
        self.assertIsNone(main.main_node_id)
        subnodes = {x.uuid: x for x in nodes[1:]}
        sub1 = subnodes[main.subnodes[0]]
        sub2 = subnodes[main.subnodes[1]]
        # Get a copy so we're not modifying the launcher's
        main = model.ProviderNode.fromZK(ctx, path=main.getPath())
        sub1 = model.ProviderNode.fromZK(ctx, path=sub1.getPath())
        sub2 = model.ProviderNode.fromZK(ctx, path=sub2.getPath())
        self.assertEqual(main.uuid, sub1.main_node_id)
        self.assertEqual(main.uuid, sub2.main_node_id)
        self.assertEqual([sub1.uuid, sub2.uuid], main.subnodes)
        self.assertEqual([], sub1.subnodes)
        self.assertEqual([], sub2.subnodes)
        self.assertEqual(main.State.SLOT_HOST, main.state)
        self.assertEqual(sub1.State.READY, sub1.state)
        self.assertEqual(sub2.State.READY, sub2.state)
        self.assertIsNotNone(sub1.request_id)
        self.assertIsNone(sub2.request_id)

        def my_advance(*args, **kw):
            raise Exception("Test exception")

        with mock.patch.object(
            zuul.launcher.server.NodescanRequest, 'advance', my_advance
        ):
            with sub2.locked(ctx):
                with sub2.activeContext(ctx):
                    sub2.unassign(ctx)
                    sub2.assign(ctx, request_id="dne", tenant_name="test")
                    # Modify the request so that the max zxid of the
                    # request cache is later than the node assignment
                    # (since the "dne" request does not exist).
                    with request.locked(ctx), request.activeContext(ctx):
                        request.priority += 1
                    sub2.setState(sub2.State.USED)

            for _ in iterate_timeout(10, "nodes to be marked failed"):
                nodes = self.launcher.api.nodes_cache.getItems()
                states = Counter(n.state for n in nodes)
                if states['failed'] == 1:
                    break

            # Delete the request so nodes can be deleted
            with sub1.locked(ctx):
                with sub1.activeContext(ctx):
                    sub1.setState(sub2.State.USED)
            request.delete(ctx)

            for _ in iterate_timeout(10, "nodes to be deleted"):
                nodes = self.launcher.api.nodes_cache.getItems()
                if len(nodes) == 0:
                    break

    @simple_layout('layouts/nodepool-subnodes-reuse.yaml',
                   enable_nodepool=True)
    def test_subnodes_building_packing(self):
        # Test that a request for a second node gets assigned to the
        # same main node even while it's building.
        with mock.patch(
            'zuul.driver.aws.awsendpoint.AwsProviderEndpoint._refresh'
        ) as refresh_mock:
            # Patch 'endpoint._refresh()' to return w/o updating
            refresh_mock.side_effect = lambda o: o
            request1 = self.requestNodes(['debian-normal'], timeout=0)
            for _ in iterate_timeout(10, "node is building"):
                nodes = self.launcher.api.nodes_cache.getItems()
                if not nodes:
                    continue
                if all(
                        n.state in (n.State.BUILDING, n.State.SLOT_HOST)
                        for n in nodes
                ):
                    break

            request2 = self.requestNodes(['debian-normal'], timeout=0)
            for _ in iterate_timeout(10, "node is building"):
                # Get a list with the main node first and the subnode last
                nodes.sort(key=lambda x: len(x.subnodes))
                nodes.reverse()
                main = nodes[0]
                sub1 = nodes[1]
                sub2 = nodes[2]

                if (main.request_id is None and
                    sub1.request_id == request1.uuid and
                    sub2.request_id == request2.uuid):
                    break

    @simple_layout('layouts/nodepool-reuse.yaml', enable_nodepool=True)
    def test_reuse_deactivate(self):
        # Deactivate a reusable node and make sure it's deleted.
        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.State.FULFILLED, request.state)

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(1, len(nodes))
        node = nodes[0]
        # Get a copy so we're not modifying the launcher's
        node = model.ProviderNode.fromZK(ctx, path=node.getPath())

        client = LauncherClient(self.zk_client, None)
        client.setNextNodeState(node, node.State.OUTDATED,
                                cache=self.launcher.api.nodes_cache)

        with node.locked(ctx):
            with node.activeContext(ctx):
                request.delete(ctx)
                node.setState(node.State.USED)

        for _ in iterate_timeout(10, "node to be deleted"):
            try:
                node.refresh(ctx)
            except NoNodeError:
                break

    @simple_layout('layouts/nodepool-subnodes-reuse.yaml',
                   enable_nodepool=True)
    def test_subnodes_reuse_deactivate_subnode(self):
        # Deactivate a reusable subnode and make sure it's deleted
        # (but not the main node).
        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.State.FULFILLED, request.state)

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(3, len(nodes))
        # Get a list with the main node first and the subnode last
        nodes.sort(key=lambda x: len(x.subnodes))
        nodes.reverse()
        main = nodes[0]
        sub1 = nodes[1]
        sub2 = nodes[2]
        # Get a copy so we're not modifying the launcher's
        main = model.ProviderNode.fromZK(ctx, path=main.getPath())
        sub1 = model.ProviderNode.fromZK(ctx, path=sub1.getPath())
        sub2 = model.ProviderNode.fromZK(ctx, path=sub2.getPath())
        node = sub2
        other = sub1

        client = LauncherClient(self.zk_client, None)
        client.setNextNodeState(node, node.State.OUTDATED,
                                cache=self.launcher.api.nodes_cache)

        with node.locked(ctx):
            with node.activeContext(ctx):
                request.delete(ctx)
                node.setState(node.State.USED)

        for _ in iterate_timeout(10, "node to be deleted"):
            try:
                node.refresh(ctx)
            except NoNodeError:
                break

        # Main and other subnode should still exist
        main.refresh(ctx)
        other.refresh(ctx)

    @simple_layout('layouts/nodepool-subnodes-reuse.yaml',
                   enable_nodepool=True)
    def test_subnodes_reuse_deactivate_main(self):
        # Deactivate the main node with subnodes and make sure they
        # are all deleted.
        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.State.FULFILLED, request.state)

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(3, len(nodes))
        # Get a list with the main node first and the subnode last
        nodes.sort(key=lambda x: len(x.subnodes))
        nodes.reverse()
        main = nodes[0]
        sub1 = nodes[1]
        sub2 = nodes[2]
        # Get a copy so we're not modifying the launcher's
        main = model.ProviderNode.fromZK(ctx, path=main.getPath())
        sub1 = model.ProviderNode.fromZK(ctx, path=sub1.getPath())
        sub2 = model.ProviderNode.fromZK(ctx, path=sub2.getPath())
        node = sub2
        other = sub1

        client = LauncherClient(self.zk_client, None)

        with node.locked(ctx):
            with node.activeContext(ctx):
                request.delete(ctx)
                node.setState(node.State.USED)

            client.setNextNodeState(main, main.State.OUTDATED,
                                    cache=self.launcher.api.nodes_cache)

            for _ in iterate_timeout(10, "other subnode to be deleted"):
                try:
                    other.refresh(ctx)
                except NoNodeError:
                    break

            # Main and in-use node should still exist
            main.refresh(ctx)
            node.refresh(ctx)

        for _ in iterate_timeout(10, "used subnode to be deleted"):
            try:
                node.refresh(ctx)
            except NoNodeError:
                break
        for _ in iterate_timeout(10, "main node to be deleted"):
            try:
                main.refresh(ctx)
            except NoNodeError:
                break
