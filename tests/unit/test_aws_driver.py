# Copyright 2024-2025 Acme Gating, LLC
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

import base64
import contextlib
import ipaddress
import time
from unittest import mock
import urllib.parse

import fixtures
from moto import mock_aws
import boto3
import botocore.exceptions
from kazoo.exceptions import NoNodeError

from zuul import model
from zuul.driver.aws import AwsDriver
from zuul.driver.aws.awsmodel import AwsProviderNode
import zuul.driver.aws.awsendpoint

from tests.fake_aws import FakeAws, FakeAwsProviderEndpoint
from tests.base import (
    AnsibleZuulTestCase,
    ResponsesFixture,
    driver_config,
    iterate_timeout,
    okay_tracebacks,
    return_data,
    simple_layout,
)
from tests.unit.test_launcher import ImageMocksFixture
from tests.unit.test_cloud_driver import BaseCloudDriverTest


def _make_ipv6_subnets(cidr_block):
    network = ipaddress.IPv6Network(cidr_block)
    # AWS only supports /64 prefix length
    return [str(sn) for sn in network.subnets(new_prefix=64)]


class AwsBaseTest(BaseCloudDriverTest):
    config_file = 'zuul-connections-nodepool.conf'
    cloud_test_image_format = 'raw'
    cloud_test_provider_name = 'aws-us-east-1-main'
    mock_aws = mock_aws()
    debian_return_data = {
        'zuul': {
            'artifacts': [
                {
                    'name': 'raw image',
                    'url': 'http://example.com/image.raw',
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
    s3_debian_return_data = {
        'zuul': {
            'artifacts': [
                {
                    'name': 'raw image',
                    'url': 's3://zuul/image.raw',
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
    s3_region_debian_return_data = {
        'zuul': {
            'artifacts': [
                {
                    'name': 'raw image',
                    'url': 's3://zuulwest/image.raw',
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

    def setUp(self):
        self.initTestConfig()
        aws_id = 'AK000000000000000000'
        aws_key = '0123456789abcdef0123456789abcdef0123456789abcdef'
        self.useFixture(
            fixtures.EnvironmentVariable('AWS_ACCESS_KEY_ID', aws_id))
        self.useFixture(
            fixtures.EnvironmentVariable('AWS_SECRET_ACCESS_KEY', aws_key))
        self.patch(zuul.driver.aws.awsendpoint, 'CACHE_TTL', 1)

        self.fake_aws = FakeAws()
        self.mock_aws.start()
        # Must start responses after mock_aws
        responses_fixture = self.useFixture(ResponsesFixture())
        self.useFixture(ImageMocksFixture(responses_fixture))

        self.ec2 = boto3.resource('ec2', region_name='us-east-1')
        self.ec2_client = boto3.client('ec2', region_name='us-east-1')
        self.s3 = boto3.resource('s3', region_name='us-east-1')
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        self.iam = boto3.resource('iam', region_name='us-east-1')
        self.s3.create_bucket(Bucket='zuul')
        location = {'LocationConstraint': 'us-west-1'}
        self.s3.create_bucket(Bucket="zuulwest",
                              CreateBucketConfiguration=location)

        # A list of args to method calls for validation
        self.run_instances_calls = []
        self.run_instances_exception = None
        self.create_fleet_calls = []
        self.create_fleet_results = []
        self.create_fleet_exception = None
        self.allocate_hosts_exception = None
        self.register_image_calls = []
        self.copy_image_calls = []

        # TEST-NET-3
        self.subnet_ids = []
        ipv6 = False
        if ipv6:
            # This is currently unused, but if moto gains IPv6 support
            # on instance creation, this may be useful.
            self.vpc = self.ec2_client.create_vpc(
                CidrBlock='203.0.113.0/24',
                AmazonProvidedIpv6CidrBlock=True)
            ipv6_cidr = self.vpc['Vpc'][
                'Ipv6CidrBlockAssociationSet'][0]['Ipv6CidrBlock']
            ipv6_subnets = _make_ipv6_subnets(ipv6_cidr)

            subnet1 = self.ec2_client.create_subnet(
                AvailabilityZone='us-east-1a',
                CidrBlock='203.0.113.64/26',
                Ipv6CidrBlock=ipv6_subnets[0],
                VpcId=self.vpc['Vpc']['VpcId'])
            self.subnet_ids.append(subnet1['Subnet']['SubnetId'])
            subnet2 = self.ec2_client.create_subnet(
                AvailabilityZone='us-east-1b',
                CidrBlock='203.0.113.128/26',
                Ipv6CidrBlock=ipv6_subnets[1],
                VpcId=self.vpc['Vpc']['VpcId'])
            self.subnet_ids.append(subnet2['Subnet']['SubnetId'])
        else:
            self.vpc = self.ec2_client.create_vpc(CidrBlock='203.0.113.0/24')
            subnet1 = self.ec2_client.create_subnet(
                AvailabilityZone='us-east-1a',
                CidrBlock='203.0.113.64/26',
                VpcId=self.vpc['Vpc']['VpcId'])
            self.subnet_ids.append(subnet1['Subnet']['SubnetId'])
            subnet2 = self.ec2_client.create_subnet(
                AvailabilityZone='us-east-1b',
                CidrBlock='203.0.113.128/26',
                VpcId=self.vpc['Vpc']['VpcId'])
            self.subnet_ids.append(subnet2['Subnet']['SubnetId'])

        self.security_group = self.ec2_client.create_security_group(
            GroupName='zuul-nodes', VpcId=self.vpc['Vpc']['VpcId'],
            Description='Zuul Nodes')
        self.security_group_id = self.security_group['GroupId']
        self.profile = self.iam.create_instance_profile(
            InstanceProfileName='not-a-real-profile')

        self.patch(AwsDriver, '_endpoint_class', FakeAwsProviderEndpoint)
        self.patch(FakeAwsProviderEndpoint,
                   '_FakeAwsProviderEndpoint__testcase', self)

        default_ec2_quotas = {
            'L-1216C47A': 100,
            'L-43DA4232': 100,
            'L-34B43A08': 100,
        }
        default_ebs_quotas = {
            'L-D18FCD1D': 100.0,
            'L-7A658B76': 100.0,
        }
        ec2_quotas = self.test_config.driver.aws.get(
            'ec2_quotas', default_ec2_quotas)
        ebs_quotas = self.test_config.driver.aws.get(
            'ebs_quotas', default_ebs_quotas)
        self.patch(FakeAwsProviderEndpoint,
                   '_FakeAwsProviderEndpoint__ec2_quotas', ec2_quotas)
        self.patch(FakeAwsProviderEndpoint,
                   '_FakeAwsProviderEndpoint__ebs_quotas', ebs_quotas)
        self.lateSetUp()
        super().setUp()

    def lateSetUp(self):
        pass

    def shutdown(self):
        super().shutdown()
        self.mock_aws.stop()


class TestAwsDriver(AwsBaseTest):

    def _assertProviderNodeAttributes(self, pnode):
        super()._assertProviderNodeAttributes(pnode)
        if checks := self.test_config.driver.aws.get('node_checks'):
            checks(self, pnode)

    def check_node_attrs(self, pnode):
        self.assertEqual(
            1000,
            self.run_instances_calls[0]['BlockDeviceMappings'][0]['Ebs']
            ['Iops'])
        self.assertEqual(
            200,
            self.run_instances_calls[0]['BlockDeviceMappings'][0]['Ebs']
            ['Throughput'])
        self.assertEqual(
            'disabled',
            self.run_instances_calls[0]['MaintenanceOptions']
            ['AutoRecovery'])
        for tag_spec in self.run_instances_calls[0]['TagSpecifications']:
            tags = {t["Key"]: t["Value"] for t in tag_spec['Tags']}
            self.assertEqual(
                "tenant-one",
                tags["tenant"])
            self.assertIn(
                "event_id",
                tags)

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    @driver_config('aws', node_checks=check_node_attrs)
    def test_aws_node_lifecycle(self):
        self._test_node_lifecycle('debian-normal')

    def check_node_tags(self, pnode):
        for tag_spec in self.run_instances_calls[0]['TagSpecifications']:
            tags = {t["Key"]: t["Value"] for t in tag_spec['Tags']}
            self.assertEqual(
                "tenant-one",
                tags["tenant"])
            self.assertIn(
                "event_id",
                tags)
            self.assertIn(
                "buildset",
                tags)
            self.assertIn(
                "foo",
                tags)

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    @driver_config('aws', node_checks=check_node_tags)
    def test_aws_node_tag_inheritance(self):
        self._test_node_lifecycle('debian-large')

    def check_dedicated_host_node_attrs(self, pnode):
        self.assertTrue(pnode.node_properties['host_id'].startswith('h-'))
        self.assertTrue(
            pnode.node_properties['ebs_volume_id'].startswith('vol-'))

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    @driver_config('aws', node_checks=check_dedicated_host_node_attrs)
    def test_aws_node_lifecycle_dedicated_host(self):
        self._test_node_lifecycle('debian-dedicated')

    def check_spot_node_attrs(self, pnode):
        # The basic test above sets few options; we set many more
        # options in the spot check (so that we don't have run a test
        # for every option).
        self.assertEqual(
            'enabled',
            self.run_instances_calls[0]['CpuOptions']['NestedVirtualization'])
        self.assertEqual(
            'spot',
            self.run_instances_calls[0]['InstanceMarketOptions']['MarketType'])
        self.assertEqual(
            'us-east-1b',
            self.run_instances_calls[0]['Placement']['AvailabilityZone'])
        self.assertEqual(
            ['testgroup'],
            self.run_instances_calls[0]['NetworkInterfaces'][0]['Groups'])
        self.assertEqual(
            1,
            self.run_instances_calls[0]['NetworkInterfaces'][0][
                'Ipv6AddressCount'])
        self.assertEqual(
            'required',
            self.run_instances_calls[0]['MetadataOptions']['HttpTokens'])
        self.assertEqual(
            self.run_instances_calls[0]['BlockDeviceMappings'][0]['Ebs']
            ['KmsKeyId'], 'alias/aws/ebs')
        self.assertEqual(
            self.run_instances_calls[0]['BlockDeviceMappings'][0]['Ebs']
            ['Encrypted'], True)
        self.assertTrue(pnode.node_properties['spot'])
        self.assertTrue(pnode.node_properties['instance_id'].startswith('i-'))
        self.assertTrue(
            pnode.node_properties['ebs_volume_id'].startswith('vol-'))
        self.assertFalse(pnode.reuse)
        instance = self.ec2_client.describe_instance_attribute(
            InstanceId=pnode.aws_instance_id,
            Attribute='userData',
        )
        expected = base64.b64encode(b'testuserdata').decode('utf8')
        self.assertEqual(expected, instance['UserData']['Value'])

    @simple_layout('layouts/aws/spot.yaml', enable_nodepool=True,
                   replace=lambda test: {
                       'subnet_ids': test.subnet_ids,
                       'iam_profile_name': test.profile.name,
                   })
    @driver_config('aws', node_checks=check_spot_node_attrs)
    def test_aws_node_lifecycle_spot(self):
        self._test_node_lifecycle('debian-normal')

    def check_fleet_node_attrs(self, pnode):
        self.assertEqual(
            'price-capacity-optimized',
            self.create_fleet_calls[0]['OnDemandOptions'][
                'AllocationStrategy'])
        self.assertTrue(pnode.node_properties['fleet'])
        self.assertTrue(pnode.node_properties['instance_id'].startswith('i-'))
        self.assertTrue(
            pnode.node_properties['ebs_volume_id'].startswith('vol-'))
        instance = self.ec2_client.describe_instance_attribute(
            InstanceId=pnode.aws_instance_id,
            Attribute='userData',
        )
        expected = base64.b64encode(b'testuserdata').decode('utf8')
        self.assertEqual(expected, instance['UserData']['Value'])

    @simple_layout('layouts/aws/fleet.yaml', enable_nodepool=True)
    @okay_tracebacks('MissingParameter')
    @driver_config('aws', node_checks=check_fleet_node_attrs)
    def test_aws_node_lifecycle_fleet(self):
        self.log.debug("Start cleanup worker")
        self.launcher.cleanup_worker.INTERVAL = 1
        self.launcher.cleanup_worker.start()
        self._test_node_lifecycle('debian-normal')
        self.waitUntilSettled()

        # Verify that we clean up unused launch templates.  Start by
        # checking that we have one from the current config.
        launch_tempaltes = self.ec2_client.\
            describe_launch_templates()['LaunchTemplates']
        self.assertEqual(len(launch_tempaltes), 1)

        # Switch to a config that has no fleet usage (spot is
        # arbitrary).
        self.commitConfigUpdate(
            'org/common-config', 'layouts/aws/spot.yaml',
            replace=lambda test: {
                'subnet_ids': test.subnet_ids,
                'iam_profile_name': test.profile.name,
            })

        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        # Verify that there are no launch templates.
        for _ in iterate_timeout(30, 'template deletion'):
            launch_templates = self.ec2_client.\
                describe_launch_templates()['LaunchTemplates']
            if len(launch_templates) == 0:
                break

    @simple_layout('layouts/aws/fleet-multi-flavor.yaml',
                   enable_nodepool=True)
    def test_aws_launch_templates_nested_virtualization(self):
        self.waitUntilSettled()

        provider = self.launcher._getProvider(
            'tenant-one', 'aws-us-east-1-main')
        endpoint = provider.getEndpoint()
        template_names = endpoint.provider_label_template_names[
            provider.canonical_name]

        expected_nested_virt = {
            'debian-no-nested-virt': False,
            'debian-with-nested-virt': True,
        }
        launch_templates = self.ec2_client.describe_launch_templates()[
            'LaunchTemplates']
        self.assertEqual(len(expected_nested_virt), len(launch_templates))

        for label_name, nested_virtualization in expected_nested_virt.items():
            launch_template_name = template_names[label_name]
            launch_template = \
                self.ec2_client.describe_launch_template_versions(
                    LaunchTemplateName=launch_template_name,
                    Versions=['$Latest'])[
                        'LaunchTemplateVersions'][0]['LaunchTemplateData']
            if nested_virtualization:
                self.assertEqual(
                    'enabled',
                    launch_template['CpuOptions']['NestedVirtualization'])
            else:
                self.assertNotIn('CpuOptions', launch_template)

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    @driver_config('aws', ec2_quotas={
        'L-1216C47A': 2,
    })
    def test_aws_quota(self):
        self._test_quota('debian-normal')

    @simple_layout('layouts/aws/resource-limits.yaml', enable_nodepool=True)
    def test_aws_resource_limits(self):
        self._test_quota('debian-normal')

    @simple_layout('layouts/aws/nodepool-image-snapshot.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        AwsBaseTest.debian_return_data,
    )
    def test_aws_diskimage_snapshot(self):
        self._test_diskimage()

    @simple_layout('layouts/aws/nodepool-image-snapshot.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        AwsBaseTest.debian_return_data,
    )
    def test_aws_diskimage_snapshot_resume(self):
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled()

        system_id = self.launcher.system.system_id
        image_tags = [
            {'Key': 'zuul_system_id', 'Value': system_id},
            {'Key': 'zuul_upload_uuid', 'Value': 'upload-uuid'},
        ]
        task = self.fake_aws.import_snapshot(
            DiskContainer={
                'Format': 'ova',
                'UserBucket': {
                    'S3Bucket': 'zuul',
                    'S3Key': 'testfile',
                }
            },
            TagSpecifications=[{
                'ResourceType': 'import-snapshot-task',
                'Tags': image_tags,
            }])
        self.fake_aws.finish_import_snapshot(task)

        provider = self.launcher._getProvider(
            'tenant-one', 'aws-us-east-1-main')
        endpoint = provider.getEndpoint()
        image = list(provider.images.values())[0]
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
        # If we reused the task, there should be two: the original
        # from test setup and our fake.
        self.assertEqual(2, len(list(endpoint._listImportSnapshotTasks())))

    @simple_layout('layouts/aws/nodepool-image-image.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        AwsBaseTest.debian_return_data,
    )
    def test_aws_diskimage_image(self):
        self._test_diskimage()

    @simple_layout('layouts/aws/nodepool-image-image.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        AwsBaseTest.debian_return_data,
    )
    def test_aws_diskimage_image_resume(self):
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled()

        system_id = self.launcher.system.system_id
        image_format = 'raw'
        bucket_name = 'zuul'
        object_filename = 's3://zuul/image.raw'
        image_tags = [
            {'Key': 'zuul_system_id', 'Value': system_id},
            {'Key': 'zuul_upload_uuid', 'Value': 'upload-uuid'},
        ]

        task = self.fake_aws.import_image(
            DiskContainers=[{
                'Format': image_format,
                'UserBucket': {
                    'S3Bucket': bucket_name,
                    'S3Key': object_filename,
                },
            }],
            TagSpecifications=[{
                'ResourceType': 'import-image-task',
                'Tags': image_tags,
            }])
        image_id, snapshot_id = self.fake_aws.finish_import_image(task)

        provider = self.launcher._getProvider(
            'tenant-one', 'aws-us-east-1-main')
        endpoint = provider.getEndpoint()
        image = list(provider.images.values())[0]
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
        # If we reused the task, there should be two: the original
        # from test setup and our fake.
        self.assertEqual(2, len(list(endpoint._listImportImageTasks())))

    @simple_layout('layouts/aws/nodepool-image-ebs-direct.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        AwsBaseTest.debian_return_data,
    )
    def test_aws_diskimage_ebs_direct(self):
        self._test_diskimage()

    @simple_layout('layouts/aws/nodepool-image-snapshot.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        AwsBaseTest.s3_debian_return_data,
    )
    def test_aws_diskimage_snapshot_import(self):
        self._test_diskimage()

    @simple_layout('layouts/aws/nodepool-image-image.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        AwsBaseTest.s3_debian_return_data,
    )
    def test_aws_diskimage_image_import(self):
        self._test_diskimage()

    @simple_layout('layouts/aws/nodepool-image-ebs-direct.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        AwsBaseTest.s3_debian_return_data,
    )
    def test_aws_diskimage_s3_download(self):
        # The ebs-direct method doesn't support an import from s3,
        # which means if we supply an s3 url, we will download it.
        bucket = self.s3.Bucket('zuul')
        bucket.put_object(Body=ImageMocksFixture.raw_body.encode('utf8'),
                          Key='image.raw')
        self._test_diskimage()

    @simple_layout('layouts/aws/nodepool-image-snapshot-region.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        AwsBaseTest.s3_region_debian_return_data,
    )
    def test_aws_diskimage_s3_region_download(self):
        # The image in a bucket in a different region should be
        # downloaded without using a direct import.
        bucket = self.s3.Bucket('zuulwest')
        bucket.put_object(Body=ImageMocksFixture.raw_body.encode('utf8'),
                          Key='image.raw')
        self._test_diskimage()

    @simple_layout('layouts/aws/nodepool-image-copy.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        AwsBaseTest.s3_debian_return_data,
    )
    def test_aws_diskimage_copy(self):
        # The image should be imported from s3 in one region and
        # copied to another.
        bucket = self.s3.Bucket('zuul')
        bucket.put_object(Body=ImageMocksFixture.raw_body.encode('utf8'),
                          Key='image.raw')
        self._test_diskimage(expected_uploads=2)
        self.assertEqual(1, len(self.copy_image_calls))

        self.launcher._runStats()
        self.assertReportedStat('zuul.uploads.state.ready',
                                value='2', kind='g')
        iname = 'review_example_com%2Forg%2Fcommon-config_debian-local'
        for region in ('us-east-1', 'us-west-1'):
            self.assertReportedStat(
                f'zuul.image.{iname}.upload.aws_aws-{region}.state.ready',
                value='1',
                kind='g')

    @simple_layout('layouts/aws/nodepool-image-copy.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        AwsBaseTest.s3_debian_return_data,
    )
    def test_aws_diskimage_copy_resume(self):
        bucket = self.s3.Bucket('zuul')
        bucket.put_object(Body=ImageMocksFixture.raw_body.encode('utf8'),
                          Key='image.raw')
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled()

        west_provider = self.launcher._getProvider(
            'tenant-one', 'aws-us-west-1-main')
        west_endpoint = west_provider.getEndpoint()

        for upload in self.launcher.image_upload_registry.getItems():
            if upload.endpoint_name == 'aws/aws-us-west-1':
                target_upload = upload

        # Tag the image since moto copy image doesn't
        system_id = self.launcher.system.system_id
        image_tags = [
            {'Key': 'zuul_system_id', 'Value': system_id},
            {'Key': 'zuul_upload_uuid', 'Value': target_upload.uuid},
        ]
        west_ec2_client = boto3.client('ec2', region_name='us-west-1')
        west_ec2_client.create_tags(
            Resources=[target_upload.external_id],
            Tags=image_tags)
        for _ in iterate_timeout(30, 'ami cache'):
            found = False
            for ami in west_endpoint._listAmis():
                if ami['Tags'] == image_tags:
                    found = True
                    break
            if found:
                break

        with self.createZKContext(None) as ctx:
            with target_upload.activeContext(ctx):
                self.log.debug("Resetting upload %s to pending",
                               target_upload)
                target_upload.state = target_upload.State.PENDING
                target_upload.external_id = None

        for _ in iterate_timeout(30, 'copy complete'):
            pending_uploads = [
                u for u in self.launcher.image_upload_registry.getItems()
                if u.state == u.State.PENDING]
            if not pending_uploads:
                break
        # If we reused the image, there should be one.
        self.assertEqual(1, len(self.copy_image_calls))

    @simple_layout('layouts/nodepool-multi-provider.yaml',
                   enable_nodepool=True)
    def test_aws_resource_cleanup(self):
        self.waitUntilSettled()
        self.launcher.cleanup_worker.INTERVAL = 1
        # This tests everything except the image imports
        # Start by setting up leaked resources
        system_id = self.launcher.system.system_id
        instance_tags = [
            {'Key': 'zuul_system_id', 'Value': system_id},
            {'Key': 'zuul_node_uuid', 'Value': '0000000042'},
        ]

        s3_tags = {
            'zuul_system_id': system_id,
            'zuul_upload_uuid': '0000000042',
        }

        reservation = self.ec2_client.run_instances(
            ImageId="ami-12c6146b", MinCount=1, MaxCount=1,
            BlockDeviceMappings=[{
                'DeviceName': '/dev/sda1',
                'Ebs': {
                    'VolumeSize': 80,
                    'DeleteOnTermination': False
                }
            }],
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': instance_tags
            }, {
                'ResourceType': 'volume',
                'Tags': instance_tags
            }]
        )
        instance_id = reservation['Instances'][0]['InstanceId']

        bucket = self.s3.Bucket('zuul')
        bucket.put_object(Body=b'hi',
                          Key='testimage',
                          Tagging=urllib.parse.urlencode(s3_tags))
        obj = self.s3.Object('zuul', 'testimage')
        # This effectively asserts the object exists
        self.s3_client.get_object_tagging(
            Bucket=obj.bucket_name, Key=obj.key)

        instance = self.ec2.Instance(instance_id)
        self.assertEqual(instance.state['Name'], 'running')

        volume_id = list(instance.volumes.all())[0].id
        volume = self.ec2.Volume(volume_id)
        self.assertEqual(volume.state, 'in-use')

        self.log.debug("Start cleanup worker")
        self.launcher.cleanup_worker.start()

        for _ in iterate_timeout(30, 'instance deletion'):
            instance = self.ec2.Instance(instance_id)
            if instance.state['Name'] == 'terminated':
                break
            time.sleep(1)

        for _ in iterate_timeout(30, 'volume deletion'):
            volume = self.ec2.Volume(volume_id)
            try:
                if volume.state == 'deleted':
                    break
            except botocore.exceptions.ClientError:
                # Probably not found
                break
            time.sleep(1)

        for _ in iterate_timeout(30, 'object deletion'):
            obj = self.s3.Object('zuul', 'testimage')
            try:
                self.s3_client.get_object_tagging(
                    Bucket=obj.bucket_name, Key=obj.key)
            except self.s3_client.exceptions.NoSuchKey:
                break
            time.sleep(1)

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_aws_resource_cleanup_import_snapshot(self):
        # This tests the import_snapshot path
        self.waitUntilSettled()
        self.launcher.cleanup_worker.INTERVAL = 1
        system_id = self.launcher.system.system_id

        # Start by setting up leaked resources
        image_tags = [
            {'Key': 'zuul_system_id', 'Value': system_id},
            {'Key': 'zuul_upload_uuid', 'Value': '0000000042'},
        ]

        task = self.fake_aws.import_snapshot(
            DiskContainer={
                'Format': 'ova',
                'UserBucket': {
                    'S3Bucket': 'zuul',
                    'S3Key': 'testfile',
                }
            },
            TagSpecifications=[{
                'ResourceType': 'import-snapshot-task',
                'Tags': image_tags,
            }])
        snapshot_id = self.fake_aws.finish_import_snapshot(task)

        register_response = self.ec2_client.register_image(
            Architecture='amd64',
            BlockDeviceMappings=[
                {
                    'DeviceName': '/dev/sda1',
                    'Ebs': {
                        'DeleteOnTermination': True,
                        'SnapshotId': snapshot_id,
                        'VolumeSize': 20,
                        'VolumeType': 'gp2',
                    },
                },
            ],
            RootDeviceName='/dev/sda1',
            VirtualizationType='hvm',
            Name='testimage',
        )
        image_id = register_response['ImageId']

        ami = self.ec2.Image(image_id)
        new_snapshot_id = ami.block_device_mappings[0]['Ebs']['SnapshotId']
        self.fake_aws.change_snapshot_id(task, new_snapshot_id)

        # Note that the resulting image and snapshot do not have tags
        # applied, so we test the automatic retagging methods in the
        # adapter.

        image = self.ec2.Image(image_id)
        self.assertEqual(image.state, 'available')

        snap = self.ec2.Snapshot(snapshot_id)
        self.assertEqual(snap.state, 'completed')

        # Now that the leaked resources exist, start the worker and
        # wait for it to clean them.
        self.log.debug("Start cleanup worker")
        self.launcher.cleanup_worker.start()

        for _ in iterate_timeout(30, 'ami deletion'):
            image = self.ec2.Image(image_id)
            try:
                # If this has a value the image was not deleted
                if image.state == 'available':
                    # Definitely not deleted yet
                    pass
            except AttributeError:
                # Per AWS API, a recently deleted image is empty and
                # looking at the state raises an AttributeFailure; see
                # https://github.com/boto/boto3/issues/2531.  The image
                # was deleted, so we continue on here
                break
            time.sleep(1)

        for _ in iterate_timeout(30, 'snapshot deletion'):
            snap = self.ec2.Snapshot(new_snapshot_id)
            try:
                if snap.state == 'deleted':
                    break
            except botocore.exceptions.ClientError:
                # Probably not found
                break
            time.sleep(1)

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_aws_resource_cleanup_import_image(self):
        # This tests the import_image path
        self.waitUntilSettled()
        self.launcher.cleanup_worker.INTERVAL = 1
        system_id = self.launcher.system.system_id

        # Start by setting up leaked resources
        image_tags = [
            {'Key': 'zuul_system_id', 'Value': system_id},
            {'Key': 'zuul_upload_uuid', 'Value': '0000000042'},
        ]

        # The image import path:
        task = self.fake_aws.import_image(
            DiskContainers=[{
                'Format': 'ova',
                'UserBucket': {
                    'S3Bucket': 'zuul',
                    'S3Key': 'testfile',
                }
            }],
            TagSpecifications=[{
                'ResourceType': 'import-image-task',
                'Tags': image_tags,
            }])
        image_id, snapshot_id = self.fake_aws.finish_import_image(task)

        # Note that the resulting image and snapshot do not have tags
        # applied, so we test the automatic retagging methods in the
        # adapter.

        image = self.ec2.Image(image_id)
        self.assertEqual(image.state, 'available')

        snap = self.ec2.Snapshot(snapshot_id)
        self.assertEqual(snap.state, 'completed')

        # Now that the leaked resources exist, start the provider and
        # wait for it to clean them.
        # Now that the leaked resources exist, start the worker and
        # wait for it to clean them.
        self.log.debug("Start cleanup worker")
        self.launcher.cleanup_worker.start()

        for _ in iterate_timeout(30, 'ami deletion'):
            image = self.ec2.Image(image_id)
            try:
                # If this has a value the image was not deleted
                if image.state == 'available':
                    # Definitely not deleted yet
                    pass
            except AttributeError:
                # Per AWS API, a recently deleted image is empty and
                # looking at the state raises an AttributeFailure; see
                # https://github.com/boto/boto3/issues/2531.  The image
                # was deleted, so we continue on here
                break
            time.sleep(1)

        for _ in iterate_timeout(30, 'snapshot deletion'):
            snap = self.ec2.Snapshot(snapshot_id)
            try:
                if snap.state == 'deleted':
                    break
            except botocore.exceptions.ClientError:
                # Probably not found
                break
            time.sleep(1)

    @simple_layout('layouts/nodepool-snapshot-expiration.yaml',
                   enable_nodepool=True)
    def test_snapshot_leak(self):
        # Ideally this would be in test_launcher, but it needs more of
        # AWS mocked out than we do there (the missing describe_*
        # paginators)
        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'])

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
            arn = node.snapshot.external_id
            with node.activeContext(ctx):
                self.log.debug("Set node to used")
                node.setState(node.State.USED)

        for _ in iterate_timeout(60, "node to be deleted"):
            try:
                node.refresh(ctx)
            except NoNodeError:
                break

        ec2 = boto3.resource('ec2', region_name='us-east-1')
        snapshot_id = arn.split('/')[1]
        snapshot = ec2.Snapshot(snapshot_id)
        tags = zuul.driver.aws.awsendpoint.tag_list_to_dict(
            snapshot.tags)
        # Assert that it has been re-tagged
        self.assertIn('zuul_system_id', tags)
        self.assertIn('zuul_expiration', tags)
        self.assertNotIn('zuul_upload_uuid', tags)

        self.launcher.cleanup_worker.INTERVAL = 1
        self.log.debug("Start cleanup worker")
        self.launcher.cleanup_worker.start()
        for _ in iterate_timeout(30, 'snapshot deletion'):
            snapshot = self.ec2.Snapshot(snapshot_id)
            try:
                if snapshot.state == 'deleted':
                    break
            except botocore.exceptions.ClientError:
                # Probably not found
                break
            time.sleep(1)

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_state_machines_instance(self):
        label_name = "debian-normal"
        provider_name = "aws-us-east-1-main"
        node_class = AwsProviderNode
        future_names = ['host_create_future', 'create_future']
        self._test_state_machines(label_name, provider_name,
                                  node_class, future_names)

    @simple_layout('layouts/nodepool.yaml', enable_nodepool=True)
    def test_state_machines_dedicated_host(self):
        label_name = "debian-dedicated"
        provider_name = "aws-us-east-1-main"
        node_class = AwsProviderNode
        future_names = ['host_create_future', 'create_future']
        self._test_state_machines(label_name, provider_name,
                                  node_class, future_names)

    @contextlib.contextmanager
    def _block_futures(self):
        with (mock.patch(
                'zuul.driver.aws.awsendpoint.AwsProviderEndpoint.'
                '_completeAllocateHost', return_value=None),
              mock.patch(
                'zuul.driver.aws.awsendpoint.AwsProviderEndpoint.'
                '_completeCreateInstance', return_value=None)):
            yield

    @simple_layout('layouts/aws/nodepool-multi-image.yaml',
                   enable_nodepool=True)
    def test_aws_multi_image(self):
        # Test that we can inherit aws attributes for both kinds of
        # images
        tenant = self.scheds.first.sched.abide.tenants.get("tenant-one")
        errors = tenant.layout.loading_errors
        self.assertEqual(len(errors), 0)

        images = tenant.layout.providers['aws-us-east-1-main'].images
        dl = images['debian-local']
        dc = images['debian-cloud']
        self.assertEqual('ebs-direct', dl.import_method)
        self.assertFalse(hasattr(dc, 'import_method'))
        self.waitUntilSettled()

    @simple_layout('layouts/aws/nodepool-error-provider.yaml',
                   enable_nodepool=True)
    def test_aws_config_error_provider(self):
        # Test a syntax error in validateConfig
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        self.assertEqual(1, len(layout.loading_errors))
        self.assertIn('Label "debian-normal": spot instances can not '
                      'be used on dedicated hosts',
                      layout.loading_errors[0].error)


class TestAwsSnapshot(AnsibleZuulTestCase, AwsBaseTest):
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

        orig_updateNode = zuul.driver.aws.awsendpoint.AwsCreateStateMachine.\
            _updateNode

        def _updateNode(self):
            orig_updateNode(self)
            self.node.connection_type = 'local'
        self.patch(zuul.driver.aws.awsendpoint.AwsCreateStateMachine,
                   '_updateNode',
                   _updateNode)

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

    def test_snapshot_e2e(self):
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/common-config',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled()
        image_cname = 'review.example.com%2Fcommon-config/debian-local'
        # We have an image, that's good enough for this test.
        artifacts = self._waitForArtifacts(image_cname, 1)
        self.assertTrue(artifacts[0].url.startswith(
            'arn:aws:ec2:us-east-1:123456789012:snapshot/snap-'))
        self._waitForUploads(image_cname, 1)

        paginator = self.ec2_client.get_paginator('describe_snapshots')
        snapshots = []
        for page in paginator.paginate(OwnerIds=['self']):
            snapshots.extend(page['Snapshots'])
        # Find our snapshot
        for snapshot in snapshots:
            tags = zuul.driver.aws.awsendpoint.tag_list_to_dict(
                snapshot['Tags'])
            if 'zuul_system_id' in tags:
                break
        # Assert that it has been re-tagged
        self.assertIn('zuul_system_id', tags)
        self.assertIn('zuul_upload_uuid', tags)
        self.assertNotIn('zuul_expiration', tags)
