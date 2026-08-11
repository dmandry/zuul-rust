# Copyright 2022, 2024 Acme Gating, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import uuid

import botocore
import boto3
import fixtures

from zuul.driver.aws.awsendpoint import AwsProviderEndpoint


def make_import_snapshot_stage_1(task_id, user_bucket, tags):
    return {
        'Architecture': 'x86_64',
        'ImportTaskId': f'import-snap-{task_id}',
        'Progress': '19',
        'SnapshotTaskDetail': {'DiskImageSize': 355024384.0,
                               'Format': 'VMDK',
                               'Status': 'active',
                               'UserBucket': user_bucket},
        'Status': 'active',
        'StatusMessage': 'converting',
        'Tags': tags,
    }


def make_import_snapshot_stage_2(task_id, snap_id, task):
    # Make a unique snapshot id that's different than the task id.
    return {
        'ImportTaskId': f'import-snap-{task_id}',
        'SnapshotTaskDetail': {'DiskImageSize': 355024384.0,
                               'Format': 'VMDK',
                               'SnapshotId': snap_id,
                               'Status': 'completed',
                               'UserBucket':
                               task['SnapshotTaskDetail']['UserBucket']},
        'Status': 'completed',
        'Tags': task['Tags'],
    }


def make_import_image_stage_1(task_id, user_bucket, tags):
    return {
        'Architecture': 'x86_64',
        'ImportTaskId': f'import-ami-{task_id}',
        'Progress': '19',
        'SnapshotDetails': [{'DiskImageSize': 355024384.0,
                             'Format': 'VMDK',
                             'Status': 'active',
                             'UserBucket': user_bucket}],
        'Status': 'active',
        'StatusMessage': 'converting',
        'Tags': tags,
    }


def make_import_image_stage_2(task_id, image_id, snap_id, task):
    # Make a unique snapshot id that's different than the task id.
    return {
        'Architecture': 'x86_64',
        'BootMode': 'legacy_bios',
        'ImageId': image_id,
        'ImportTaskId': f'import-ami-{task_id}',
        'LicenseType': 'BYOL',
        'Platform': 'Linux',
        'SnapshotDetails': [{'DeviceName': '/dev/sda1',
                             'DiskImageSize': 355024384.0,
                             'Format': 'VMDK',
                             'SnapshotId': snap_id,
                             'Status': 'completed',
                             'UserBucket':
                             task['SnapshotDetails'][0]['UserBucket']}],
        'Status': 'completed',
        'Tags': task['Tags'],
    }


class RetryPaginateFixture(fixtures.Fixture):

    def _setUp(self):
        super()._setUp()

        # Patch boto pagination since moto describe_instances is not
        # thread-safe
        orig_paginate = botocore.paginate.Paginator.paginate

        def retry_paginate(*args, **kw):
            for x in range(10):
                try:
                    return orig_paginate(*args, **kw)
                except RuntimeError:
                    # describe_instances can fail if the underlying
                    # dicts change while it renders its
                    # template. Ignore the error and retry.
                    pass

        self.useFixture(fixtures.MonkeyPatch(
            'botocore.paginate.Paginator.paginate',
            retry_paginate))


class ImportSnapshotTaskPaginator:
    log = logging.getLogger("nodepool.FakeAws")

    def __init__(self, fake):
        self.fake = fake

    def paginate(self, **kw):
        tasks = list(self.fake.tasks.values())
        tasks = [t for t in tasks if 'import-snap' in t['ImportTaskId']]
        if 'ImportTaskIds' in kw:
            tasks = [t for t in tasks
                     if t['ImportTaskId'] in kw['ImportTaskIds']]
        # A page of tasks
        ret = [{'ImportSnapshotTasks': tasks}]

        # Move the task along
        for task in tasks:
            if task['Status'] != 'completed':
                self.fake.finish_import_snapshot(task)
        return ret


class ImportImageTaskPaginator:
    log = logging.getLogger("nodepool.FakeAws")

    def __init__(self, fake):
        self.fake = fake

    def paginate(self, **kw):
        tasks = list(self.fake.tasks.values())
        tasks = [t for t in tasks if 'import-ami' in t['ImportTaskId']]
        if 'ImportTaskIds' in kw:
            tasks = [t for t in tasks
                     if t['ImportTaskId'] in kw['ImportTaskIds']]
            if not tasks:
                raise botocore.exceptions.ClientError(
                    {'Error': {'Code': 'InvalidConversionTaskId.Malformed'}},
                    'DescribeImportImageTasks')
        # A page of tasks
        ret = [{'ImportImageTasks': tasks}]

        # Move the task along
        for task in tasks:
            if task['Status'] != 'completed':
                self.fake.finish_import_image(task)
        return ret


class FakeAws:
    log = logging.getLogger("nodepool.FakeAws")

    def __init__(self):
        self.tasks = {}
        self.ec2 = boto3.resource('ec2', region_name='us-east-1')
        self.ec2_client = boto3.client('ec2', region_name='us-east-1')
        self.fail_import_count = 0

    def import_snapshot(self, *args, **kw):
        while self.fail_import_count:
            self.fail_import_count -= 1
            raise botocore.exceptions.ClientError(
                {'Error': {'Code': 'ResourceCountLimitExceeded'}},
                'ImportSnapshot')
        task_id = uuid.uuid4().hex
        task = make_import_snapshot_stage_1(
            task_id,
            kw['DiskContainer']['UserBucket'],
            kw['TagSpecifications'][0]['Tags'])
        self.tasks[task_id] = task
        return task

    def finish_import_snapshot(self, task):
        task_id = task['ImportTaskId'].split('-')[-1]

        # Make a Volume to simulate the import finishing
        volume = self.ec2_client.create_volume(
            Size=80,
            AvailabilityZone='us-east-1')
        snap_id = self.ec2_client.create_snapshot(
            VolumeId=volume['VolumeId'],
        )["SnapshotId"]

        t2 = make_import_snapshot_stage_2(task_id, snap_id, task)
        self.tasks[task_id] = t2
        return snap_id

    def import_image(self, *args, **kw):
        while self.fail_import_count:
            self.fail_import_count -= 1
            raise botocore.exceptions.ClientError(
                {'Error': {'Code': 'ResourceCountLimitExceeded'}},
                'ImportImage')
        task_id = uuid.uuid4().hex
        task = make_import_image_stage_1(
            task_id,
            kw['DiskContainers'][0]['UserBucket'],
            kw['TagSpecifications'][0]['Tags'])
        self.tasks[task_id] = task
        return task

    def finish_import_image(self, task):
        task_id = task['ImportTaskId'].split('-')[-1]

        # Make an AMI to simulate the import finishing
        reservation = self.ec2_client.run_instances(
            ImageId="ami-12c6146b", MinCount=1, MaxCount=1)
        instance = reservation["Instances"][0]
        instance_id = instance["InstanceId"]

        response = self.ec2_client.create_image(
            InstanceId=instance_id,
            Name=f'import-ami-{task_id}',
        )

        image_id = response["ImageId"]
        self.ec2_client.describe_images(ImageIds=[image_id])["Images"][0]

        volume = self.ec2_client.create_volume(
            Size=80,
            AvailabilityZone='us-east-1')
        snap_id = self.ec2_client.create_snapshot(
            VolumeId=volume['VolumeId'],
            Description=f'imported volume import-ami-{task_id}',
        )["SnapshotId"]

        t2 = make_import_image_stage_2(task_id, image_id, snap_id, task)
        self.tasks[task_id] = t2
        return (image_id, snap_id)

    def change_snapshot_id(self, task, snapshot_id):
        # Given a task, update its snapshot id; the moto
        # register_image mock doesn't honor the snapshot_id we pass
        # in.
        task_id = task['ImportTaskId'].split('-')[-1]
        self.tasks[task_id]['SnapshotTaskDetail']['SnapshotId'] = snapshot_id

    def get_paginator(self, name):
        if name == 'describe_import_image_tasks':
            return ImportImageTaskPaginator(self)
        if name == 'describe_import_snapshot_tasks':
            return ImportSnapshotTaskPaginator(self)
        raise NotImplementedError()

    def _listAmis(self):
        return list(self.ec2.images.filter())

    def _listSnapshots(self):
        return list(self.ec2.snapshots.filter())


class FakeAwsProviderEndpoint(AwsProviderEndpoint):
    IMAGE_UPLOAD_SLEEP = 1

    def _getClients(self):
        super()._getClients()

        # Note: boto3 doesn't handle ipv6 addresses correctly
        # when in fake mode so we need to intercept the
        # run_instances call and validate the args we supply.
        def _fake_run_instances(*args, **kwargs):
            self.__testcase.run_instances_calls.append(kwargs)
            if self.__testcase.run_instances_exception:
                raise self.__testcase.run_instances_exception
            return self.ec2_client.run_instances_orig(*args, **kwargs)

        # Moto doesn't handle all features correctly (e.g.
        # instance-requirements, volume attributes) when creating
        # fleet in fake mode, we need to intercept the create_fleet
        # call and validate the args we supply. Results are also
        # intercepted for validate instance attributes
        def _fake_create_fleet(*args, **kwargs):
            self.__testcase.create_fleet_calls.append(kwargs)
            if self.__testcase.create_fleet_exception:
                raise self.__testcase.create_fleet_exception
            result = self.ec2_client.create_fleet_orig(*args, **kwargs)
            self.__testcase.create_fleet_results.append(result)
            return result

        def _fake_allocate_hosts(*args, **kwargs):
            if self.__testcase.allocate_hosts_exception:
                raise self.__testcase.allocate_hosts_exception
            return self.ec2_client.allocate_hosts_orig(*args, **kwargs)

        # The ImdsSupport parameter isn't handled by moto
        def _fake_register_image(*args, **kwargs):
            self.__testcase.register_image_calls.append(kwargs)
            return self.ec2_client.register_image_orig(*args, **kwargs)

        # Just so we can count how many times it's called
        def _fake_copy_image(*args, **kwargs):
            self.__testcase.copy_image_calls.append(kwargs)
            return self.ec2_client.copy_image_orig(*args, **kwargs)

        def _fake_get_paginator(*args, **kwargs):
            try:
                return self.__testcase.fake_aws.get_paginator(*args, **kwargs)
            except NotImplementedError:
                return self.ec2_client.get_paginator_orig(*args, **kwargs)

        self.ec2_client.run_instances_orig = self.ec2_client.run_instances
        self.ec2_client.run_instances = _fake_run_instances
        self.ec2_client.create_fleet_orig = self.ec2_client.create_fleet
        self.ec2_client.create_fleet = _fake_create_fleet
        self.ec2_client.allocate_hosts_orig = self.ec2_client.allocate_hosts
        self.ec2_client.allocate_hosts = _fake_allocate_hosts
        self.ec2_client.register_image_orig = self.ec2_client.register_image
        self.ec2_client.register_image = _fake_register_image
        self.ec2_client.copy_image_orig = self.ec2_client.copy_image
        self.ec2_client.copy_image = _fake_copy_image
        self.ec2_client.import_snapshot = \
            self.__testcase.fake_aws.import_snapshot
        self.ec2_client.import_image = \
            self.__testcase.fake_aws.import_image
        self.ec2_client.get_paginator_orig = self.ec2_client.get_paginator
        self.ec2_client.get_paginator = _fake_get_paginator

        # moto does not mock service-quotas, so we do it ourselves:
        def _fake_get_service_quota(ServiceCode, QuotaCode, *args, **kwargs):
            if ServiceCode == 'ec2':
                qdict = self.__ec2_quotas
            elif ServiceCode == 'ebs':
                qdict = self.__ebs_quotas
            else:
                raise NotImplementedError(
                    f"Quota code {ServiceCode} not implemented")
            return {'Quota': {'Value': qdict.get(QuotaCode)}}
        self.aws_quotas.get_service_quota = _fake_get_service_quota

        def _fake_list_service_quotas(ServiceCode, *args, **kwargs):
            if ServiceCode == 'ec2':
                qdict = self.__ec2_quotas
            elif ServiceCode == 'ebs':
                qdict = self.__ebs_quotas
            else:
                raise NotImplementedError(
                    f"Quota code {ServiceCode} not implemented")
            quotas = []
            for code, value in qdict.items():
                quotas.append(
                    {'Value': value, 'QuotaCode': code}
                )
            return {'Quotas': quotas}
        self.aws_quotas.list_service_quotas = _fake_list_service_quotas
