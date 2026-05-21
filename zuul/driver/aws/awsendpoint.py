# Copyright 2018 Red Hat
# Copyright 2022-2025 Acme Gating, LLC
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
import cachetools.func
import copy
import functools
import hashlib
import uuid
import json
import logging
import math
import queue
import random
import re
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

import boto3
import botocore.config
import botocore.exceptions
import botocore.session
import botocore.utils
from botocore.credentials import AssumeRoleWithWebIdentityProvider

from zuul import exceptions
from zuul.driver.aws.awsmodel import AwsResource, AwsInstance
from zuul.driver.aws.const import (
    HOST_QUOTA_CODES,
    INSTANCE_QUOTA_CODES,
    ON_DEMAND,
    SPOT,
    VOLUME_QUOTA_CODES,
)
from zuul.driver.aws.util import tag_dict_to_list, tag_list_to_dict
from zuul.driver.util import (
    ImageUploader,
    LazyExecutorTTLCache,
    RateLimiter,
)
from zuul.model import QuotaInformation
from zuul.provider import (
    BaseImageCopyJob,
    BaseImageImportJob,
    BaseImageUploadJob,
    BaseProviderEndpoint,
    statemachine
)


CACHE_TTL = 10
SERVICE_QUOTA_CACHE_TTL = 300
KIB = 1024
GIB = 1024 ** 3


class AwsDeleteStateMachine(statemachine.StateMachine):
    HOST_RELEASING_START = 'start releasing host'
    HOST_RELEASING = 'releasing host'
    INSTANCE_DELETING_START = 'start deleting instance'
    INSTANCE_DELETING = 'deleting instance'

    def __init__(self, endpoint, node, log):
        self.log = log
        self.endpoint = endpoint
        self.node = node
        super().__init__(node.delete_state)

        # Restore local objects
        self.host = None
        if node.aws_dedicated_host_id:
            host = dict(HostId=node.aws_dedicated_host_id)
            self.host = self.endpoint._refreshDelete(host)
        self.instance = None
        if node.aws_instance_id:
            instance = dict(InstanceId=node.aws_instance_id)
            self.instance = self.endpoint._refreshDelete(instance)

    def advance(self):
        if self.state == self.START:
            if self.instance:
                self.state = self.INSTANCE_DELETING_START
            elif self.host:
                self.state = self.HOST_RELEASING_START
            else:
                self.state = self.COMPLETE

        if self.state == self.INSTANCE_DELETING_START:
            self.instance = self.endpoint._deleteInstance(
                self.node.aws_instance_id, self.log)
            self.state = self.INSTANCE_DELETING

        if self.state == self.INSTANCE_DELETING:
            self.instance = self.endpoint._refreshDelete(self.instance)
            if self.instance is None:
                if self.host:
                    self.state = self.HOST_RELEASING_START
                else:
                    self.state = self.COMPLETE

        if self.state == self.HOST_RELEASING_START:
            self.host = self.endpoint._releaseHost(
                self.node.aws_dedicated_host_id, self.log)
            self.state = self.HOST_RELEASING

        if self.state == self.HOST_RELEASING:
            self.host = self.endpoint._refreshDelete(self.host)
            if self.host is None:
                self.state = self.COMPLETE

        if self.state == self.COMPLETE:
            self.complete = True


class AwsCreateStateMachine(statemachine.StateMachine):
    HOST_ALLOCATING_START = 'start allocating host'
    HOST_ALLOCATING = 'allocating host'
    INSTANCE_CREATING_START = 'start creating instance'
    INSTANCE_CREATING = 'creating instance'

    def __init__(self, provider, endpoint, node, hostname, label,
                 flavor, image, image_external_id, tags, log):
        self.log = log
        self.provider = provider
        self.endpoint = endpoint
        self.node = node
        self.tags = tags.copy()
        self.tags['Name'] = hostname
        self.hostname = hostname
        self.label = label
        self.flavor = flavor
        self.image = image
        self.public_ipv4 = None
        self.public_ipv6 = None
        self.nic = None
        self.host_create_future = None
        self.create_future = None
        super().__init__(node.create_state)
        self.attempts = node.create_state.get("attempts", 0)
        self.image_external_id = node.create_state.get(
            "image_external_id", image_external_id)

        # Restore local objects
        self.node.quota = self.endpoint.getQuotaForLabel(
            self.label, self.flavor)

        if self.state in (
                self.HOST_ALLOCATING_START, self.INSTANCE_CREATING_START):
            for instance in self.endpoint.listInstances():
                # TODO: use the nodepool node id tag instead
                if instance.metadata.get("Name") == hostname:
                    self.node.aws_instance_id = instance.aws_instance_id
                    self.node.aws_dedicated_host_id =\
                        instance.aws_dedicated_host_id
            if self.node.aws_instance_id:
                self.state = self.INSTANCE_CREATING
            elif self.node.aws_dedicated_host_id:
                self.state = self.HOST_ALLOCATING

        self.host = None
        if self.node.aws_dedicated_host_id:
            host = dict(
                HostId=self.node.aws_dedicated_host_id,
                State=dict(Name='pending'),
            )
            self.host = self.endpoint._refresh(host)
        self.instance = None
        if self.node.aws_instance_id:
            instance = dict(
                InstanceId=self.node.aws_instance_id,
                State=dict(Name='pending'),
            )
            self.instance = self.endpoint._refresh(instance)

    def toDict(self):
        data = super().toDict()
        data.update(
            attempts=self.attempts,
            image_external_id=self.image_external_id,
        )
        return data

    def advance(self):
        if self.state == self.START:
            self.node.node_properties['fleet'] = bool(self.flavor.fleet)
            self.node.node_properties['spot'] = bool(
                self.flavor.market_type == 'spot')
            # If this label uses a launch template (fleet api), get
            # its name now since the label and the set of launch
            # templates may change later.
            label_template_names = \
                self.endpoint.provider_label_template_names.get(
                    self.provider.canonical_name, {})
            self.node.aws_launch_template_name =\
                label_template_names.get(self.label.name)

            if self.flavor.dedicated_host:
                self.state = self.HOST_ALLOCATING_START
            else:
                self.state = self.INSTANCE_CREATING_START

        if self.state == self.HOST_ALLOCATING_START:
            if not self.host_create_future:
                self.host_create_future = self.endpoint._submitAllocateHost(
                    self.label, self.flavor, self.tags, self.hostname,
                    self.log)

            host = self.endpoint._completeAllocateHost(
                self.host_create_future)
            if host is None:
                return
            self.host = host
            self.node.aws_dedicated_host_id = host['HostId']
            self.state = self.HOST_ALLOCATING

        if self.state == self.HOST_ALLOCATING:
            self.host = self.endpoint._refresh(self.host)

            state = self.host['State'].lower()
            if state == 'available':
                self.node.aws_dedicated_host_id = self.host['HostId']
                self.node.node_properties['host_id'] = self.host['HostId']
                self.state = self.INSTANCE_CREATING_START
            elif state in [
                    'permanent-failure', 'released',
                    'released-permanent-failure']:
                raise exceptions.LaunchStatusException(
                    f"Host in {state} state")
            else:
                return

        if self.state == self.INSTANCE_CREATING_START:
            if not self.create_future:
                self.create_future = self.endpoint._submitCreateInstance(
                    self.provider, self.label, self.flavor, self.image,
                    self.image_external_id, self.tags, self.hostname,
                    self.node.aws_dedicated_host_id,
                    self.node.aws_launch_template_name,
                    self.log)

            instance = self.endpoint._completeCreateInstance(
                self.create_future)
            if instance is None:
                return
            self.instance = instance
            self.node.aws_instance_id = instance['InstanceId']
            self.node.node_properties['instance_id'] = instance['InstanceId']
            self.state = self.INSTANCE_CREATING

        if self.state == self.INSTANCE_CREATING:
            self.instance = self.endpoint._refresh(self.instance)

            if self.instance['State']['Name'].lower() == "running":
                self._updateNode()
                self.state = self.COMPLETE
            elif self.instance['State']['Name'].lower() == "terminated":
                raise exceptions.LaunchStatusException(
                    "Instance in terminated state")
            else:
                return

        if self.state == self.COMPLETE:
            self.complete = True

    def _updateNode(self):
        self.node.quota = self.endpoint.getQuotaForLabel(
            self.label, self.flavor, self.instance['InstanceType'])
        instance = AwsInstance(self.endpoint.region, self.instance,
                               self.host, self.node.quota)

        # Extract the ID of the EBS volume that was created together
        # with the instance
        for attachment in self.instance['BlockDeviceMappings']:
            if attachment['DeviceName'] == self.instance['RootDeviceName']:
                self.node.node_properties['ebs_volume_id'] = (
                    attachment['Ebs']['VolumeId']
                )
                break

        self.node.updateFromInstance(instance)


class AwsSnapshotStateMachine(statemachine.StateMachine):
    SNAPSHOT_PENDING = 'snapshot pending'

    def __init__(self, endpoint, node, log):
        self.log = log
        self.endpoint = endpoint
        self.node = node
        initial_state = node.snapshot.snapshot_state
        super().__init__(initial_state)
        self.snapshot_id = initial_state.get('snapshot_id', None)
        self.tags = node.snapshot.tags
        # Restore local variables
        self.account_id = self.endpoint._getAccountId()

        self.instance = None
        if node.aws_instance_id:
            instance = dict(InstanceId=node.aws_instance_id)
            self.instance = self.endpoint._refresh(instance)

        self.volume_id = None
        if self.instance:
            for attachment in self.instance['BlockDeviceMappings']:
                if attachment['DeviceName'] == self.instance['RootDeviceName']:
                    self.volume_id = attachment['Ebs']['VolumeId']
                    break

        if self.snapshot_id:
            snapshot = dict(SnapshotId=self.snapshot_id)
            self.snapshot = self.endpoint._refresh(snapshot)

    def toDict(self):
        data = super().toDict()
        data.update(
            snapshot_id=self.snapshot_id,
        )
        return data

    def advance(self):
        if self.state == self.START:
            self.snapshot = self.endpoint._createSnapshot(
                self.volume_id, self.tags)
            self.snapshot_id = self.snapshot['SnapshotId']
            self.state = self.SNAPSHOT_PENDING

        if self.state == self.SNAPSHOT_PENDING:
            self.snapshot = self.endpoint._refresh(self.snapshot)

            state = self.snapshot['State'].lower()
            if state == "completed":
                self.state = self.COMPLETE
            elif state == "pending":
                return
            else:
                raise Exception(f"Snapshot in {state} state")

        if self.state == self.COMPLETE:
            arn = (f'arn:aws:ec2:{self.endpoint.region}:{self.account_id}:'
                   f'snapshot/{self.snapshot_id}')
            self.complete = True
            return arn


class AwsImageCopyJob(BaseImageCopyJob):
    def __init__(self, endpoint,
                 provider_image, image_name,
                 image_format, metadata,
                 source_region):
        super().__init__()
        self.endpoint = endpoint
        self.provider_image = provider_image
        self.image_name = image_name
        self.image_format = image_format
        self.metadata = metadata
        self.source_region = source_region

    def run(self, external_id):
        self.endpoint.log.debug(f"Copying image {self.image_name} "
                                f"from {external_id} in {self.source_region}")

        image_id = self.endpoint._copyImage(
            self.provider_image, self.image_name,
            self.image_format, self.metadata,
            self.source_region, external_id)
        return image_id


class AwsImageImportJob(BaseImageImportJob):
    def __init__(self, endpoint,
                 provider_image, image_name,
                 image_format, metadata,
                 bucket_name, object_filename,
                 snapshot_id, timeout):
        super().__init__()
        self.endpoint = endpoint
        self.provider_image = provider_image
        self.image_name = image_name
        self.image_format = image_format
        self.metadata = metadata
        self.bucket_name = bucket_name
        self.object_filename = object_filename
        self.snapshot_id = snapshot_id
        self.timeout = timeout

    def run(self):
        import_method = self.provider_image.import_method
        if self.snapshot_id:
            import_method = 'registration'
        self.endpoint.log.debug(f"Importing image {self.image_name} "
                                f"via {import_method}")

        delete_object = False
        if import_method == 'image':
            image_id = self.endpoint._uploadImageImage(
                self.provider_image, self.image_name,
                self.image_format, self.metadata,
                self.bucket_name, self.object_filename, self.timeout,
                delete_object)
        elif import_method == 'snapshot':
            image_id = self.endpoint._uploadImageSnapshot(
                self.provider_image, self.image_name,
                self.image_format, self.metadata,
                self.bucket_name, self.object_filename, self.timeout,
                delete_object)
        elif import_method == 'registration':
            image_id = self.endpoint._importImageSnapshot(
                self.provider_image, self.image_name,
                self.snapshot_id, self.metadata)
        else:
            raise Exception("Unknown image import method")
        return image_id


class AwsImageUploadJob(BaseImageUploadJob):
    def __init__(self, endpoint,
                 provider_image, image_name,
                 image_format, metadata,
                 bucket_name, timeout):
        super().__init__()
        self.endpoint = endpoint
        self.provider_image = provider_image
        self.image_name = image_name
        self.image_format = image_format
        self.metadata = metadata
        self.bucket_name = bucket_name
        self.timeout = timeout

    def run(self, filename):
        self.endpoint.log.debug(f"Uploading image {self.image_name} "
                                f"via {self.provider_image.import_method}")

        delete_object = True
        if self.provider_image.import_method != 'ebs-direct':
            # Upload image to S3
            object_filename = self.endpoint._uploadImageToS3(
                self.image_name, filename, self.image_format,
                self.metadata, self.bucket_name)
        if self.provider_image.import_method == 'image':
            image_id = self.endpoint._uploadImageImage(
                self.provider_image, self.image_name,
                self.image_format, self.metadata,
                self.bucket_name, object_filename, self.timeout,
                delete_object)
        elif self.provider_image.import_method == 'snapshot':
            image_id = self.endpoint._uploadImageSnapshot(
                self.provider_image, self.image_name,
                self.image_format, self.metadata,
                self.bucket_name, object_filename, self.timeout,
                delete_object)
        elif self.provider_image.import_method == 'ebs-direct':
            image_id = self.endpoint._uploadImageSnapshotEBS(
                self.provider_image, self.image_name, filename,
                self.image_format, self.metadata, self.timeout)
        else:
            raise Exception("Unknown image import method")
        return image_id


class EbsSnapshotUploader(ImageUploader):
    segment_size = 512 * KIB

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.segment_count = 0

    def shouldRetryException(self, exception):
        # Strictly speaking, ValidationException is only retryable
        # if we get a particular message, but that's impractical
        # to reproduce for testing.
        # https://docs.aws.amazon.com/ebs/latest/userguide/error-retries.html
        ex = self.endpoint.ebs_client.exceptions
        if isinstance(exception, (
                ex.RequestThrottledException,
                ex.InternalServerException,
                ex.ValidationException,
        )):
            return True
        return False

    def _rateLimited(self, func):
        def rateLimitedFunc(*args, **kw):
            with self.endpoint.rate_limiter:
                return func(*args, **kw)
        return rateLimitedFunc

    def uploadSegment(self, segment):
        # There is a default limit of 1000 put requests/second.
        # Actual value is available as a service quota.  We don't
        # expect to hit this.  If we do, and we need to rate-limit, we
        # will need to coordinate with other builders.
        # https://docs.aws.amazon.com/ebs/latest/userguide/ebs-resource-quotas.html
        data = segment.data
        if len(data) < self.segment_size:
            # Add zeros if the last block is smaller since the
            # block size in AWS is constant.
            data = data.ljust(self.segment_size, b'\0')
        checksum = hashlib.sha256(data)
        checksum_base64 = base64.b64encode(checksum.digest()).decode('utf-8')

        response = self.retry(
            self.endpoint.ebs_client.put_snapshot_block,
            SnapshotId=self.snapshot_id,
            BlockIndex=segment.index,
            BlockData=data,
            DataLength=len(data),
            Checksum=checksum_base64,
            ChecksumAlgorithm='SHA256',
        )
        if (response['Checksum'] != checksum_base64):
            raise Exception("Checksums do not match; received "
                            f"{response['Checksum']} expected {checksum}")
        self.segment_count += 1

    def startUpload(self):
        # This is used by AWS to ensure idempotency across retries
        token = uuid.uuid4().hex
        # Volume size is in GiB
        size = math.ceil(self.size / GIB)
        response = self.retry(
            self._rateLimited(self.endpoint.ebs_client.start_snapshot),
            VolumeSize=size,
            ClientToken=token,
            Tags=tag_dict_to_list(self.metadata),
        )
        self.snapshot_id = response['SnapshotId']

    def finishUpload(self):
        while True:
            response = self.retry(
                self._rateLimited(self.endpoint.ebs_client.complete_snapshot),
                SnapshotId=self.snapshot_id,
                ChangedBlocksCount=self.segment_count,
            )
            if response['Status'] == 'error':
                raise Exception("Snapshot in error state")
            if response['Status'] == 'completed':
                break
            self.checkTimeout()
        return self.size, self.snapshot_id

    def abortUpload(self):
        try:
            self.finishUpload()
        except Exception:
            pass
        with self.endpoint.rate_limiter:
            snapshot_id = getattr(self, 'snapshot_id', None)
            if snapshot_id:
                self.endpoint.ec2_client.delete_snapshot(
                    SnapshotId=self.snapshot_id)


class AwsProviderEndpoint(BaseProviderEndpoint):
    """An AWS Endpoint corresponds to a single AWS region, and can include
    multiple availability zones."""

    IMAGE_UPLOAD_SLEEP = 30
    LAUNCH_TEMPLATE_PREFIX = 'zuul-launch-template'

    def __init__(self, zk_client, driver, connection, region, system_id):
        name = f'{connection.connection_name}-{region}'
        super().__init__(zk_client, driver, connection, name, system_id)
        self.log = logging.getLogger(f"zuul.aws.{self.name}")
        self.region = region
        self.invalid_instance_types = set()

        self.rate_limiter = RateLimiter(self.name,
                                        connection.rate)
        # Non mutating requests can be made more often at 10x the rate
        # of mutating requests by default.
        self.non_mutating_rate_limiter = RateLimiter(self.name,
                                                     connection.rate * 10.0)
        # Experimentally, this rate limit refreshes tokens at
        # something like 0.16/second, so if we operated at the rate
        # limit, it would take us almost a minute to determine the
        # quota.  Instead, we're going to just use the normal provider
        # rate and rely on caching to avoid going over the limit.  At
        # the of writing, we'll issue bursts of 5 requests every 5
        # minutes.
        self.quota_service_rate_limiter = RateLimiter(self.name,
                                                      connection.rate)

        # Wrap these instance methods with a per-instance LRU cache so
        # that we don't leak memory over time when the adapter is
        # occasionally replaced.
        # TODO: This may be able to be a different kind of cache now
        self._getInstanceType = functools.lru_cache(maxsize=None)(
            self._getInstanceType)
        self._getImage = functools.lru_cache(maxsize=None)(
            self._getImage)
        self._getAccountId = functools.lru_cache(maxsize=None)(
            self._getAccountId)

        self.image_id_by_filter_cache = cachetools.TTLCache(
            maxsize=8192, ttl=(5 * 60))

        self.provider_label_template_names = {}
        self._post_config_run = False
        # In listResources, we reconcile AMIs which appear to be
        # imports but have no nodepool tags, however it's possible
        # that these aren't nodepool images.  If we determine that's
        # the case, we'll add their ids here so we don't waste our
        # time on that again.
        self.not_our_images = set()
        self.not_our_snapshots = set()

    def _getSessionFromCredentials(self):
        return boto3.Session(
            aws_access_key_id=self.connection.access_key_id,
            aws_secret_access_key=self.connection.secret_access_key,
            profile_name=self.connection.profile,
        )

    def _getSessionFromWebIdentity(self):
        # Boto3 does not allow passing web_identity_token_file as a
        # constructor argument to the boto3.Session object, so we
        # create the identity provider ourselves and add it to the
        # botocore session.  This allows us to benefit from the
        # automatic token refresh without needing to use a config file
        # or environment variables (though those are supported as
        # well).
        botocore_session = botocore.session.Session()
        profile = self.connection.profile or 'default'
        config = {
            'profiles': {
                profile: {
                    'role_arn': self.connection.role_arn,
                    'web_identity_token_file':
                    self.connection.web_identity_token_file,
                }
            }
        }

        # Based on code in botocore/credentials.py (ASL2)
        def client_creator(service_name, **kwargs):
            create_client_kwargs = {'region_name': self.region}
            create_client_kwargs.update(**kwargs)
            return botocore.utils.create_nested_client(
                botocore_session, service_name, **create_client_kwargs
            )
        cache = {}
        id_provider = AssumeRoleWithWebIdentityProvider(
            load_config=lambda: config,
            client_creator=client_creator,
            cache=cache,
            profile_name=profile,
            disable_env_vars=False,
        )
        resolver = botocore_session.get_component('credential_provider')
        first = resolver.providers[0].METHOD
        resolver.insert_before(first, id_provider)
        return boto3.Session(botocore_session=botocore_session)

    def _getClients(self):
        # A separate method for unit tests
        if self.connection.role_arn:
            self.aws = self._getSessionFromWebIdentity()
        else:
            self.aws = self._getSessionFromCredentials()
        client_config = botocore.config.Config(retries={
            "total_max_attempts": 5,
            # standard mode covers more error cases, especially
            # TooManyRequestsException
            "mode": "standard",
        })
        self.ec2_client = self.aws.client(
            "ec2",
            region_name=self.region,
            endpoint_url=self.connection.endpoint_url,
            config=client_config,
        )
        self.s3 = self.aws.resource(
            's3',
            region_name=self.region,
            endpoint_url=self.connection.endpoint_url,
        )
        self.s3_client = self.aws.client(
            's3',
            region_name=self.region,
            endpoint_url=self.connection.endpoint_url,
            config=client_config,
        )
        self.s3_global_client = self.aws.client(
            's3',
            endpoint_url=self.connection.endpoint_url,
            config=client_config,
        )
        self.aws_quotas = self.aws.client(
            'service-quotas',
            region_name=self.region,
            endpoint_url=self.connection.endpoint_url,
            config=client_config,
        )
        self.ebs_client = self.aws.client(
            'ebs',
            region_name=self.region,
            endpoint_url=self.connection.endpoint_url,
            config=client_config,
        )
        self.sts_client = self.aws.client(
            'sts',
            region_name=self.region,
            endpoint_url=self.connection.endpoint_url,
            config=client_config,
        )

    def startEndpoint(self):
        self._running = True
        self.log.debug("Starting AWS endpoint")
        self._getClients()

        # AWS has a default rate limit for creating instances that
        # works out to a sustained 2 instances/sec, but the actual
        # create instance API call takes 1 second or more.  If we want
        # to achieve faster than 1 instance/second throughput, we need
        # to parallelize create instance calls, so we set up a
        # threadworker to do that.

        # A little bit of a heuristic here to set the worker count.
        # It appears that AWS typically takes 1-1.5 seconds to execute
        # a create API call.  Figure out how many we have to do in
        # parallel in order to run at the rate limit, then quadruple
        # that for headroom.  Max out at 8 so we don't end up with too
        # many threads.  In practice, this will be 8 with the default
        # values, and only less if users slow down the rate.
        workers = max(min(int(self.connection.rate * 4), 8), 1)
        self.log.info("Create executor with max workers=%s", workers)
        self.create_executor = ThreadPoolExecutor(
            thread_name_prefix=f'aws-create-{self.name}',
            max_workers=workers)

        # We can batch delete instances using the AWS API, so to do
        # that, create a queue for deletes, and a thread to process
        # the queue.  It will be greedy and collect as many pending
        # instance deletes as possible to delete together.  Typically
        # under load, that will mean a single instance delete followed
        # by larger batches.  That strikes a balance between
        # responsiveness and efficiency.  Reducing the overall number
        # of requests leaves more time for create instance calls.
        self.delete_host_queue = queue.Queue()
        self.delete_instance_queue = queue.Queue()
        self.delete_thread = threading.Thread(
            name=f'aws-delete-{self.name}',
            target=self._deleteThread)
        self.delete_thread.start()

        workers = 10
        self.log.info("Create executor with max workers=%s", workers)
        self.api_executor = ThreadPoolExecutor(
            thread_name_prefix=f'aws-api-{self.name}',
            max_workers=workers)

        # Use a lazy TTL cache for these.  This uses the TPE to
        # asynchronously update the cached values, meanwhile returning
        # the previous cached data if available.  This means every
        # call after the first one is instantaneous.
        self._listHosts = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(
                self._listHosts)
        self._listInstances = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(
                self._listInstances)
        self._listVolumes = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(
                self._listVolumes)
        self._listAmis = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(
                self._listAmis)
        self._listSnapshots = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(
                self._listSnapshots)
        self._listObjects = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(
                self._listObjects)
        self._listLaunchTemplates = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(
                self._listLaunchTemplates)
        self._listEC2Quotas = LazyExecutorTTLCache(
            SERVICE_QUOTA_CACHE_TTL, self.api_executor)(
                self._listEC2Quotas)
        self._listEBSQuotas = LazyExecutorTTLCache(
            SERVICE_QUOTA_CACHE_TTL, self.api_executor)(
                self._listEBSQuotas)

    def stopEndpoint(self):
        self.log.debug("Stopping AWS endpoint")
        self.create_executor.shutdown()
        self.api_executor.shutdown()
        self._running = False
        self.delete_host_queue.put(None)
        self.delete_instance_queue.put(None)
        self.delete_thread.join()

    def postConfig(self, provider):
        self._createLaunchTemplates(provider)
        self._post_config_run = True

    def listRequiredInfrastructureResources(self):
        # In case the cleanup worker runs before our first full
        # configuration (eg, if there are errors at startup).
        if not self._post_config_run:
            raise Exception("Unable to list resources "
                            "until configuration complete")

        in_use_template_names = set()
        for provider_name, template_names in \
                self.provider_label_template_names.items():
            for label_name, template_name in template_names.items():
                in_use_template_names.add(template_name)

        for template in self._listLaunchTemplates():
            template_name = template['LaunchTemplateName']
            if template_name not in in_use_template_names:
                continue
            tags = tag_list_to_dict(template.get('Tags', []))
            yield AwsResource(tags,
                              AwsResource.TYPE_LAUNCH_TEMPLATE,
                              template_name)

    def listResources(self, providers):
        bucket_names = set()
        for provider in providers:
            if provider.object_storage:
                if bn := provider.object_storage.get('bucket_name'):
                    bucket_names.add(bn)
        self._tagSnapshots()
        self._tagAmis()

        for host in self._listHosts():
            try:
                if host['State'].lower() in [
                        "released", "released-permanent-failure"]:
                    continue
            except botocore.exceptions.ClientError:
                continue
            yield AwsResource(tag_list_to_dict(host.get('Tags')),
                              AwsResource.TYPE_HOST,
                              host['HostId'])
        for instance in self._listInstances():
            try:
                if instance['State']['Name'].lower() == "terminated":
                    continue
            except botocore.exceptions.ClientError:
                continue
            yield AwsResource(tag_list_to_dict(instance.get('Tags')),
                              AwsResource.TYPE_INSTANCE,
                              instance['InstanceId'])
        for volume in self._listVolumes():
            try:
                if volume['State'].lower() == "deleted":
                    continue
            except botocore.exceptions.ClientError:
                continue
            yield AwsResource(tag_list_to_dict(volume.get('Tags')),
                              AwsResource.TYPE_VOLUME, volume['VolumeId'])
        for ami in self._listAmis():
            try:
                if ami['State'].lower() == "deleted":
                    continue
            except botocore.exceptions.ClientError:
                continue
            yield AwsResource(tag_list_to_dict(ami.get('Tags')),
                              AwsResource.TYPE_AMI, ami['ImageId'])
        for snap in self._listSnapshots():
            try:
                if snap['State'].lower() == "deleted":
                    continue
            except botocore.exceptions.ClientError:
                continue
            yield AwsResource(tag_list_to_dict(snap.get('Tags')),
                              AwsResource.TYPE_SNAPSHOT, snap['SnapshotId'])
        for bucket_name in bucket_names:
            for obj in self._listObjects(bucket_name):
                with self.non_mutating_rate_limiter:
                    try:
                        tags = self.s3_client.get_object_tagging(
                            Bucket=obj.bucket_name, Key=obj.key)
                    except botocore.exceptions.ClientError:
                        continue
                yield AwsResource(tag_list_to_dict(tags['TagSet']),
                                  AwsResource.TYPE_OBJECT, obj.key,
                                  bucket_name=bucket_name)
        for template in self._listLaunchTemplates():
            yield AwsResource(tag_list_to_dict(template.get('Tags')),
                              AwsResource.TYPE_LAUNCH_TEMPLATE,
                              template['LaunchTemplateName'])

    def deleteResource(self, resource):
        self.log.info(f"Deleting leaked {resource.type}: {resource.id}")
        if resource.type == AwsResource.TYPE_HOST:
            self._releaseHost(resource.id, immediate=True)
        if resource.type == AwsResource.TYPE_INSTANCE:
            self._deleteInstance(resource.id, immediate=True)
        if resource.type == AwsResource.TYPE_VOLUME:
            self._deleteVolume(resource.id)
        if resource.type == AwsResource.TYPE_AMI:
            self._deleteAmi(resource.id)
        if resource.type == AwsResource.TYPE_SNAPSHOT:
            self._deleteSnapshot(resource.id)
        if resource.type == AwsResource.TYPE_OBJECT:
            self._deleteObject(resource.bucket_name, resource.id)
        if resource.type == AwsResource.TYPE_LAUNCH_TEMPLATE:
            self._deleteLaunchTemplate(resource.id)

    def listInstances(self):
        volumes = {}
        for volume in self._listVolumes():
            volumes[volume['VolumeId']] = volume
        for instance in self._listInstances():
            if instance['State']["Name"].lower() == "terminated":
                continue
            # For now, we are optimistically assuming that when an
            # instance is launched on a dedicated host, it is not
            # counted against instance quota.  That may be overly
            # optimistic.  If it is, then we will merge the two quotas
            # below rather than switch.
            # Additionally, we are using the instance as a proxy for
            # the host.  It would be more correct to also list hosts
            # here to include hosts with no instances.  But since our
            # support for dedicated hosts is currently 1:1 with
            # instances, this should be sufficient.
            if instance['Placement'].get('HostId'):
                # Dedicated host
                quota = self._getQuotaForHostType(
                    instance['InstanceType'])
            else:
                quota = self._getQuotaForInstanceType(
                    instance['InstanceType'],
                    SPOT if instance.get('InstanceLifecycle') == 'spot'
                    else ON_DEMAND)
            for attachment in instance['BlockDeviceMappings']:
                volume_id = attachment['Ebs']['VolumeId']
                volume = volumes.get(volume_id)
                if volume is None:
                    self.log.warning(
                        "Volume %s of instance %s could not be found",
                        volume_id, instance['InstanceId'])
                    continue
                quota.add(self._getQuotaForVolume(volume))

            yield AwsInstance(self.region, instance, None, quota)

    def getQuotaForLabel(self, label, flavor, instance_type=None):
        # This should not rely on the client connection, only the
        # quota cache.

        # When using the Fleet API, we may need to fill in quota
        # information from the actual instance, so this internal
        # method operates on the label alone or label+instance.

        # For now, we are optimistically assuming that when an
        # instance is launched on a dedicated host, it is not counted
        # against instance quota.  That may be overly optimistic.  If
        # it is, then we will merge the two quotas below rather than
        # switch.
        if flavor.dedicated_host:
            key = f'host-{flavor.instance_type}'
            quota = self.quota_cache.getResource(key)
        elif flavor.fleet and instance_type is None:
            # For fleet API, do not check quota before launch the instance
            quota = QuotaInformation(instances=1)
        else:
            check_instance_type = flavor.instance_type or instance_type
            if flavor.market_type == 'spot':
                key = f'spot-instance-{check_instance_type}'
            else:
                key = f'market-instance-{check_instance_type}'
            quota = self.quota_cache.getResource(key)
        if label.volume_type:
            quota.add(self._constructQuotaForVolumeType(
                label.volume_type,
                storage=label.volume_size,
                iops=label.iops))
        return quota

    def refreshQuotaLimits(self, update):
        if self.quota_cache.hasLimits() and not update:
            return False
        ec2_quotas = self._listEC2Quotas()
        ebs_quotas = self._listEBSQuotas()
        args = dict()
        args.update(ec2_quotas)
        args.update(ebs_quotas)
        limits = QuotaInformation(**args)
        self.quota_cache.setLimits(limits)
        return True

    def refreshQuotaForLabel(self, label, flavor, update):
        if flavor.dedicated_host:
            key = f'host-{flavor.instance_type}'
            if update or not self.quota_cache.hasResource(key):
                quota = self._getQuotaForHostType(flavor.instance_type)
                if quota is not None:
                    self.quota_cache.setResource(key, quota)
        else:
            instance_types = []
            if flavor.instance_type:
                instance_types.append(flavor.instance_type)
            if flavor.fleet:
                instance_types.extend(flavor.fleet.get('instance_types', []))
            for instance_type in instance_types:
                if flavor.market_type == 'spot':
                    key = f'spot-instance-{instance_type}'
                else:
                    key = f'market-instance-{instance_type}'
                if update or not self.quota_cache.hasResource(key):
                    quota = self._getQuotaForInstanceType(
                        instance_type,
                        SPOT if flavor.market_type == 'spot' else ON_DEMAND)
                    if quota is not None:
                        self.quota_cache.setResource(key, quota)

    def _getBucketRegion(self, bucket_name):
        data = self.s3_global_client.get_bucket_location(Bucket=bucket_name)
        # None means us-east-1 for s3 buckets
        return data['LocationConstraint'] or 'us-east-1'

    def downloadUrl(self, url, path):
        if not url.startswith('s3://'):
            return None

        url_parts = urllib.parse.urlparse(url)
        bucket_name = url_parts.netloc
        object_filename = url_parts.path.lstrip('/')

        self.log.debug("Downloading %s to %s", url, path)
        self.s3_client.download_file(bucket_name, object_filename, path)
        return path

    def getImageImportJob(self, url, provider_image, image_name,
                          image_format, metadata, md5, sha256):
        if not (url.startswith('s3://') or url.startswith('arn:')):
            return None

        if provider_image.import_method == 'image':
            # There is no IMDS support option for the import_image call
            if provider_image.imds_support == 'v2.0':
                raise Exception("IMDSv2 requires 'snapshot' import method")
        elif provider_image.import_method == 'snapshot':
            pass
        elif provider_image.import_method == 'ebs-direct':
            return None
        else:
            raise Exception("Unknown image import method")

        timeout = provider_image.import_timeout
        if url.startswith('s3://'):
            url_parts = urllib.parse.urlparse(url)
            bucket_name = url_parts.netloc
            object_filename = url_parts.path.lstrip('/')

            if self._getBucketRegion(bucket_name) != self.region:
                return None

            return AwsImageImportJob(
                self,
                provider_image, image_name,
                image_format, metadata,
                bucket_name, object_filename,
                None, timeout)
        elif url.startswith('arn:'):
            url_parts = url.split(':')
            region = url_parts[3]

            if region != self.region:
                return None

            snapshot_id = url_parts[5].split('/')[1]
            return AwsImageImportJob(
                self,
                provider_image, image_name,
                image_format, metadata,
                None, None,
                snapshot_id, timeout)

    def getImageCopyJob(self, source_provider, provider_image, image_name,
                        image_format, metadata, md5, sha256):
        source_endpoint = source_provider.endpoint
        if not isinstance(source_endpoint, AwsProviderEndpoint):
            return None
        source_region = source_endpoint.region
        if source_region == self.region:
            return None
        return AwsImageCopyJob(
            self,
            provider_image, image_name,
            image_format, metadata,
            source_region)

    def getImageUploadJob(self, provider_image, image_name,
                          image_format, metadata, md5, sha256, bucket_name):

        # There is no IMDS support option for the import_image call
        if (provider_image.import_method == 'image' and
            provider_image.imds_support == 'v2.0'):
            raise Exception("IMDSv2 requires 'snapshot' import method")

        if provider_image.import_method not in (
                'image', 'snapshot', 'ebs-direct'):
            raise Exception("Unknown image import method")

        timeout = provider_image.import_timeout

        return AwsImageUploadJob(
            self,
            provider_image, image_name,
            image_format, metadata,
            bucket_name, timeout)

    def _uploadImageToS3(self, image_name, filename, image_format, metadata,
                         bucket_name):
        bucket = self.s3.Bucket(bucket_name)
        object_filename = f'{image_name}.{image_format}'
        extra_args = {'Tagging': urllib.parse.urlencode(metadata)}

        with open(filename, "rb") as fobj:
            with self.rate_limiter:
                bucket.upload_fileobj(fobj, object_filename,
                                      ExtraArgs=extra_args)
        return object_filename

    def _registerImage(self, provider_image, image_name, metadata,
                       volume_size, snapshot_id):
        # Register the snapshot as an AMI
        with self.rate_limiter:
            bdm = {
                'DeviceName': '/dev/sda1',
                'Ebs': {
                    'DeleteOnTermination': True,
                    'SnapshotId': snapshot_id,
                    'VolumeSize': volume_size,
                    'VolumeType': provider_image.volume_type,
                },
            }
            if provider_image.iops:
                bdm['Ebs']['Iops'] = provider_image.iops
            if provider_image.throughput:
                bdm['Ebs']['Throughput'] = provider_image.throughput

            args = dict(
                Architecture=provider_image.architecture,
                BlockDeviceMappings=[bdm],
                RootDeviceName='/dev/sda1',
                VirtualizationType='hvm',
                EnaSupport=provider_image.ena_support,
                Name=image_name,
                TagSpecifications=[
                    {
                        'ResourceType': 'image',
                        'Tags': tag_dict_to_list(metadata),
                    },
                ]
            )
            if provider_image.imds_support == 'v2.0':
                args['ImdsSupport'] = 'v2.0'
            return self.ec2_client.register_image(**args)

    def _uploadImageSnapshotEBS(self, provider_image, image_name, filename,
                                image_format, metadata, timeout):
        # Import snapshot
        uploader = EbsSnapshotUploader(self, self.log, filename, image_name,
                                       metadata)
        self.log.debug(f"Importing {image_name} as EBS snapshot")
        volume_size, snapshot_id = uploader.upload(timeout)

        register_response = self._registerImage(
            provider_image, image_name, metadata, volume_size, snapshot_id,
        )

        self.log.debug(f"Upload of {image_name} complete as "
                       f"{register_response['ImageId']}")
        return register_response['ImageId']

    def _getOrStartImportSnapshot(self, image_format, bucket_name,
                                  object_filename, timeout_ts, metadata):
        for task in self._listImportSnapshotTasks():
            candidate_tags = tag_list_to_dict(task['Tags'])
            if metadata.items() <= candidate_tags.items():
                task_id = task['ImportTaskId']
                self.log.debug("Found existing import task %s",
                               task_id)
                return task_id
        while True:
            try:
                with self.rate_limiter:
                    import_snapshot_task = self.ec2_client.import_snapshot(
                        DiskContainer={
                            'Format': image_format,
                            'UserBucket': {
                                'S3Bucket': bucket_name,
                                'S3Key': object_filename,
                            },
                        },
                        TagSpecifications=[
                            {
                                'ResourceType': 'import-snapshot-task',
                                'Tags': tag_dict_to_list(metadata),
                            },
                        ]
                    )
                    break
            except botocore.exceptions.ClientError as error:
                if (error.response['Error']['Code'] ==
                    'ResourceCountLimitExceeded'):
                    if time.time() < timeout_ts:
                        self.log.warning("AWS error: '%s' will retry",
                                         str(error))
                        time.sleep(self.IMAGE_UPLOAD_SLEEP)
                        continue
                raise
        task_id = import_snapshot_task['ImportTaskId']
        self.log.debug("Started new import task %s",
                       task_id)
        return task_id

    def _uploadImageSnapshot(self, provider_image, image_name,
                             image_format, metadata,
                             bucket_name, object_filename, timeout,
                             delete_object):
        # Import snapshot
        self.log.debug(f"Importing {image_name} as snapshot")
        timeout_ts = time.time()
        if timeout:
            timeout_ts += timeout

        # Get the task id of an already running snapshot import if we
        # have one, otherwise start a new one.
        task_id = self._getOrStartImportSnapshot(
            image_format, bucket_name,
            object_filename, timeout_ts, metadata)

        paginator = self.ec2_client.get_paginator(
            'describe_import_snapshot_tasks')
        done = False
        while not done:
            time.sleep(self.IMAGE_UPLOAD_SLEEP)
            with self.non_mutating_rate_limiter:
                for page in paginator.paginate(ImportTaskIds=[task_id]):
                    for task in page['ImportSnapshotTasks']:
                        if task['SnapshotTaskDetail']['Status'].lower() in (
                                'completed', 'deleted'):
                            done = True
                            break

        if delete_object:
            self.log.debug(f"Deleting {image_name} from S3")
            with self.rate_limiter:
                self.s3.Object(bucket_name, object_filename).delete()

        if task['SnapshotTaskDetail']['Status'].lower() != 'completed':
            raise Exception(f"Error uploading image: {task}")

        # Tag the snapshot
        with self.non_mutating_rate_limiter:
            resp = self.ec2_client.describe_snapshots(
                SnapshotIds=[task['SnapshotTaskDetail']['SnapshotId']])
            snap = resp['Snapshots'][0]
        try:
            with self.rate_limiter:
                self.ec2_client.create_tags(
                    Resources=[task['SnapshotTaskDetail']['SnapshotId']],
                    Tags=task['Tags'])
        except Exception:
            self.log.exception("Error tagging snapshot:")

        volume_size = provider_image.volume_size or snap['VolumeSize']
        snapshot_id = task['SnapshotTaskDetail']['SnapshotId']
        register_response = self._registerImage(
            provider_image, image_name, metadata, volume_size, snapshot_id,
        )

        self.log.debug(f"Upload of {image_name} complete as "
                       f"{register_response['ImageId']}")
        return register_response['ImageId']

    def _getOrStartImportImage(self, provider_image, image_format,
                               bucket_name, object_filename,
                               timeout_ts, metadata):
        for task in self._listImportImageTasks():
            candidate_tags = tag_list_to_dict(task['Tags'])
            if metadata.items() <= candidate_tags.items():
                task_id = task['ImportTaskId']
                self.log.debug("Found existing import task %s",
                               task_id)
                return task_id
        while True:
            try:
                with self.rate_limiter:
                    import_image_task = self.ec2_client.import_image(
                        Architecture=provider_image.architecture,
                        DiskContainers=[{
                            'Format': image_format,
                            'UserBucket': {
                                'S3Bucket': bucket_name,
                                'S3Key': object_filename,
                            },
                        }],
                        TagSpecifications=[
                            {
                                'ResourceType': 'import-image-task',
                                'Tags': tag_dict_to_list(metadata),
                            },
                        ]
                    )
                    break
            except botocore.exceptions.ClientError as error:
                if (error.response['Error']['Code'] ==
                    'ResourceCountLimitExceeded'):
                    if time.time() < timeout_ts:
                        self.log.warning("AWS error: '%s' will retry",
                                         str(error))
                        time.sleep(self.IMAGE_UPLOAD_SLEEP)
                        continue
                raise
        task_id = import_image_task['ImportTaskId']
        self.log.debug("Started new import task %s",
                       task_id)
        return task_id

    def _uploadImageImage(self, provider_image, image_name,
                          image_format, metadata,
                          bucket_name, object_filename, timeout,
                          delete_object):
        # Import image as AMI
        self.log.debug(f"Importing {image_name} as AMI")
        timeout_ts = time.time()
        if timeout:
            timeout_ts += timeout
        task_id = self._getOrStartImportImage(
            provider_image, image_format, bucket_name,
            object_filename, timeout_ts, metadata)

        paginator = self.ec2_client.get_paginator(
            'describe_import_image_tasks')
        done = False
        while not done:
            time.sleep(self.IMAGE_UPLOAD_SLEEP)
            with self.non_mutating_rate_limiter:
                for page in paginator.paginate(ImportTaskIds=[task_id]):
                    for task in page['ImportImageTasks']:
                        if task['Status'].lower() in ('completed', 'deleted'):
                            done = True
                            break

        if delete_object:
            self.log.debug(f"Deleting {image_name} from S3")
            with self.rate_limiter:
                self.s3.Object(bucket_name, object_filename).delete()

        if task['Status'].lower() != 'completed':
            raise Exception(f"Error uploading image: {task}")

        # Tag the AMI
        try:
            with self.rate_limiter:
                self.ec2_client.create_tags(
                    Resources=[task['ImageId']],
                    Tags=task['Tags'])
        except Exception:
            self.log.exception("Error tagging AMI:")

        # Tag the snapshot
        try:
            with self.rate_limiter:
                self.ec2_client.create_tags(
                    Resources=[task['SnapshotDetails'][0]['SnapshotId']],
                    Tags=task['Tags'])
        except Exception:
            self.log.exception("Error tagging snapshot:")

        self.log.debug(f"Upload of {image_name} complete as {task['ImageId']}")
        # Last task returned from paginator above
        return task['ImageId']

    def _importImageSnapshot(self, provider_image, image_name,
                             snapshot_id, metadata):
        # Import from an existing snapshot
        self.log.debug(f"Importing {image_name} as snapshot")

        snapshot = dict(SnapshotId=snapshot_id)
        snapshot = self._refresh(snapshot)

        # Re-Tag the snapshot with the upload tags
        try:
            with self.rate_limiter:
                self.ec2_client.delete_tags(
                    Resources=[snapshot_id],
                    Tags=snapshot['Tags'],
                )
            with self.rate_limiter:
                self.ec2_client.create_tags(
                    Resources=[snapshot_id],
                    Tags=tag_dict_to_list(metadata),
                )
        except Exception:
            self.log.exception("Error tagging snapshot:")

        volume_size = provider_image.volume_size or snapshot['VolumeSize']
        register_response = self._registerImage(
            provider_image, image_name, metadata, volume_size, snapshot_id,
        )

        self.log.debug(f"Import of {image_name} complete as "
                       f"{register_response['ImageId']}")
        return register_response['ImageId']

    def _getOrStartCopyImage(self, image_name, source_ami,
                             source_region, metadata):
        for ami in self._listAmis():
            candidate_tags = tag_list_to_dict(ami['Tags'])
            if metadata.items() <= candidate_tags.items():
                image_id = ami['ImageId']
                self.log.debug("Found existing image %s",
                               image_id)
                return image_id
        with self.rate_limiter:
            resp = self.ec2_client.copy_image(
                Name=image_name,
                SourceImageId=source_ami,
                SourceRegion=source_region,
                CopyImageTags=False,
                TagSpecifications=[
                    {
                        'ResourceType': 'image',
                        'Tags': tag_dict_to_list(metadata),
                    },
                ]
            )
        image_id = resp['ImageId']
        self.log.debug("Copied new image %s", image_id)
        return image_id

    def _copyImage(self, provider_image, image_name,
                   image_format, metadata,
                   source_region, source_ami):
        image_id = self._getOrStartCopyImage(image_name, source_ami,
                                             source_region, metadata)
        while True:
            time.sleep(self.IMAGE_UPLOAD_SLEEP)
            for ami in self._listAmis():
                if ami['ImageId'] == image_id:
                    state = ami['State'].lower()
                    if state == "available":
                        return image_id
                    if state == "failed":
                        reason = ami['StateReason']
                        raise Exception(f"Image copy failed: {reason}")
                    break

    def deleteImage(self, external_id):
        snaps = set()
        self.log.debug(f"Deleting image {external_id}")
        for ami in self._listAmis():
            if ami['ImageId'] == external_id:
                for bdm in ami.get('BlockDeviceMappings', []):
                    snapid = bdm.get('Ebs', {}).get('SnapshotId')
                    if snapid:
                        snaps.add(snapid)
        self._deleteAmi(external_id)
        for snapshot_id in snaps:
            self._deleteSnapshot(snapshot_id)

    # Local implementation below

    def _tagAmis(self):
        # There is no way to tag imported AMIs, so this routine
        # "eventually" tags them.  We look for any AMIs without tags
        # and we copy the tags from the associated snapshot or image
        # import task.
        to_examine = []
        for ami in self._listAmis():
            if ami['ImageId'] in self.not_our_images:
                continue
            if ami.get('Tags'):
                continue

            # This has no tags, which means it's either not a nodepool
            # image, or it's a new one which doesn't have tags yet.
            if ami['Name'].startswith('import-ami-'):
                task = self._getImportImageTask(ami['Name'])
                if task:
                    # This was an import image (not snapshot) so let's
                    # try to find tags from the import task.
                    tags = tag_list_to_dict(task.get('Tags'))
                    if (tags.get('zuul_system_id') == self.system_id):
                        # Copy over tags
                        self.log.debug(
                            "Copying tags from import task %s to AMI",
                            ami['Name'])
                        with self.rate_limiter:
                            self.ec2_client.create_tags(
                                Resources=[ami['ImageId']],
                                Tags=task['Tags'])
                        continue

            # This may have been a snapshot import; try to copy over
            # any tags from the snapshot import task, otherwise, mark
            # it as an image we can ignore in future runs.
            if len(ami.get('BlockDeviceMappings', [])) < 1:
                self.not_our_images.add(ami['ImageId'])
                continue
            bdm = ami['BlockDeviceMappings'][0]
            ebs = bdm.get('Ebs')
            if not ebs:
                self.not_our_images.add(ami['ImageId'])
                continue
            snapshot_id = ebs.get('SnapshotId')
            if not snapshot_id:
                self.not_our_images.add(ami['ImageId'])
                continue
            to_examine.append((ami, snapshot_id))
        if not to_examine:
            return

        # We have images to examine; get a list of import tasks so
        # we can copy the tags from the import task that resulted in
        # this image.
        task_map = {}
        for task in self._listImportSnapshotTasks():
            detail = task['SnapshotTaskDetail']
            task_snapshot_id = detail.get('SnapshotId')
            if not task_snapshot_id:
                continue
            task_map[task_snapshot_id] = task['Tags']

        for ami, snapshot_id in to_examine:
            tags = tag_list_to_dict(task_map.get(snapshot_id))
            if not tags:
                self.not_our_images.add(ami['ImageId'])
                continue
            if (tags.get('zuul_system_id') == self.system_id):
                # Copy over tags
                self.log.debug(
                    "Copying tags from import task to image %s",
                    ami['ImageId'])
                with self.rate_limiter:
                    self.ec2_client.create_tags(
                        Resources=[ami['ImageId']],
                        Tags=task_map.get(snapshot_id))
            else:
                self.not_our_images.add(ami['ImageId'])

    def _tagSnapshots(self):
        # See comments for _tagAmis
        to_examine = []
        for snap in self._listSnapshots():
            if snap['SnapshotId'] in self.not_our_snapshots:
                continue
            try:
                if snap.get('Tags'):
                    continue
            except botocore.exceptions.ClientError:
                # We may have cached a snapshot that doesn't exist
                continue

            if 'import-ami' in snap.get('Description', ''):
                match = re.match(r'.*?(import-ami-\w*)',
                                 snap.get('Description', ''))
                task = None
                if match:
                    task_id = match.group(1)
                    task = self._getImportImageTask(task_id)
                if task:
                    # This was an import image (not snapshot) so let's
                    # try to find tags from the import task.
                    tags = tag_list_to_dict(task.get('Tags'))
                    if (tags.get('zuul_system_id') == self.system_id):
                        # Copy over tags
                        self.log.debug(
                            f"Copying tags from import task {task_id}"
                            " to snapshot")
                        with self.rate_limiter:
                            self.ec2_client.create_tags(
                                Resources=[snap['SnapshotId']],
                                Tags=task['Tags'])
                        continue

            # This may have been a snapshot import; try to copy over
            # any tags from the snapshot import task.
            to_examine.append(snap)

        if not to_examine:
            return

        # We have snapshots to examine; get a list of import tasks so
        # we can copy the tags from the import task that resulted in
        # this snapshot.
        task_map = {}
        for task in self._listImportSnapshotTasks():
            detail = task['SnapshotTaskDetail']
            task_snapshot_id = detail.get('SnapshotId')
            if not task_snapshot_id:
                continue
            task_map[task_snapshot_id] = task['Tags']

        for snap in to_examine:
            tags = tag_list_to_dict(task_map.get(snap['SnapshotId']))
            if not tags:
                self.not_our_snapshots.add(snap['SnapshotId'])
                continue
            if (tags.get('zuul_system_id') == self.system_id):
                # Copy over tags
                self.log.debug(
                    "Copying tags from import task to snapshot %s",
                    snap['SnapshotId'])
                with self.rate_limiter:
                    self.ec2_client.create_tags(
                        Resources=[snap['SnapshotId']],
                        Tags=task_map.get(snap['SnapshotId']))
            else:
                self.not_our_snapshots.add(snap['SnapshotId'])

    def _getImportImageTask(self, task_id):
        paginator = self.ec2_client.get_paginator(
            'describe_import_image_tasks')
        with self.non_mutating_rate_limiter:
            try:
                for page in paginator.paginate(ImportTaskIds=[task_id]):
                    for task in page['ImportImageTasks']:
                        # Return the first and only task
                        return task
            except botocore.exceptions.ClientError as error:
                if (error.response['Error']['Code'] ==
                    'InvalidConversionTaskId.Malformed'):
                    # In practice, this can mean that the task no
                    # longer exists
                    pass
                else:
                    raise
        return None

    def _listImportImageTasks(self):
        paginator = self.ec2_client.get_paginator(
            'describe_import_image_tasks')
        with self.non_mutating_rate_limiter:
            for page in paginator.paginate():
                for task in page['ImportImageTasks']:
                    yield task

    def _listImportSnapshotTasks(self):
        paginator = self.ec2_client.get_paginator(
            'describe_import_snapshot_tasks')
        with self.non_mutating_rate_limiter:
            for page in paginator.paginate():
                for task in page['ImportSnapshotTasks']:
                    yield task

    instance_key_re = re.compile(r'([a-z\-]+)\d.*')

    def _getQuotaCodeForInstanceType(self, instance_type, market_type_option):
        m = self.instance_key_re.match(instance_type)
        if m:
            key = m.group(1)
            code = INSTANCE_QUOTA_CODES.get(key)
            if code:
                return code[market_type_option]
            self.log.warning(
                "Unknown quota code for instance type: %s",
                instance_type)
        return None

    def _getQuotaForInstanceType(self, instance_type, market_type_option):
        if instance_type in self.invalid_instance_types:
            return None
        try:
            itype = self._getInstanceType(instance_type)
            cores = itype['InstanceTypes'][0]['VCpuInfo']['DefaultCores']
            vcpus = itype['InstanceTypes'][0]['VCpuInfo']['DefaultVCpus']
            ram = itype['InstanceTypes'][0]['MemoryInfo']['SizeInMiB']
            code = self._getQuotaCodeForInstanceType(instance_type,
                                                     market_type_option)
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'InvalidInstanceType':
                self.log.warning("Invalid instance type: %s", instance_type)
                self.invalid_instance_types.add(instance_type)
            else:
                self.log.exception("Error querying instance type: %s",
                                   instance_type)
            return None
        # We include cores to match the overall cores quota (which may
        # be set as a tenant resource limit), and include vCPUs for the
        # specific AWS quota code which in for a specific instance
        # type. With two threads per core, the vCPU number is
        # typically twice the number of cores. AWS service quotas are
        # implemented in terms of vCPUs.
        args = dict(cores=cores, ram=ram, instances=1)
        if code:
            args[code] = vcpus

        return QuotaInformation(**args)

    host_key_re = re.compile(r'([a-z\d\-]+)\..*')

    def _getQuotaCodeForHostType(self, host_type):
        m = self.host_key_re.match(host_type)
        if m:
            key = m.group(1)
            code = HOST_QUOTA_CODES.get(key)
            if code:
                return code
            self.log.warning(
                "Unknown quota code for host type: %s",
                host_type)
        return None

    def _getQuotaForHostType(self, host_type):
        code = self._getQuotaCodeForHostType(host_type)
        args = dict(instances=1)
        if code:
            args[code] = 1

        return QuotaInformation(**args)

    def _getQuotaForVolume(self, volume):
        volume_type = volume['VolumeType']
        vquota_codes = VOLUME_QUOTA_CODES.get(volume_type, {})
        args = {}
        if 'iops' in vquota_codes and volume.get('Iops'):
            args[vquota_codes['iops']] = volume['Iops']
        if 'storage' in vquota_codes and volume.get('Size'):
            args[vquota_codes['storage']] = volume['Size']
        return QuotaInformation(**args)

    def _constructQuotaForVolumeType(self, volume_type,
                                     storage=None, iops=None):
        # This doesn't need to consult the API
        vquota_codes = VOLUME_QUOTA_CODES.get(volume_type, {})
        args = {}
        if 'iops' in vquota_codes and iops is not None:
            args[vquota_codes['iops']] = iops
        if 'storage' in vquota_codes and storage is not None:
            args[vquota_codes['storage']] = storage
        return QuotaInformation(**args)

    # This method is wrapped with an LRU cache in the constructor.
    def _getInstanceType(self, instance_type):
        with self.non_mutating_rate_limiter:
            self.log.debug(
                f"Getting information for instance type {instance_type}")
            return self.ec2_client.describe_instance_types(
                InstanceTypes=[instance_type])

    def _refresh(self, obj):
        if 'InstanceId' in obj:
            for instance in self._listInstances():
                if instance['InstanceId'] == obj['InstanceId']:
                    return instance
        elif 'HostId' in obj:
            for host in self._listHosts():
                if host['HostId'] == obj['HostId']:
                    return host
        elif 'SnapshotId' in obj:
            for snapshot in self._listSnapshots():
                if snapshot['SnapshotId'] == obj['SnapshotId']:
                    return snapshot
        return obj

    def _refreshDelete(self, obj):
        if obj is None:
            return obj

        if 'InstanceId' in obj:
            for instance in self._listInstances():
                if instance['InstanceId'] == obj['InstanceId']:
                    if instance['State']['Name'].lower() == "terminated":
                        return None
                    return instance
        elif 'HostId' in obj:
            for host in self._listHosts():
                if host['HostId'] == obj['HostId']:
                    if host['State'].lower() in [
                            'released', 'released-permanent-failure']:
                        return None
                    return host
        return None

    def _listServiceQuotas(self, service_code):
        with self.quota_service_rate_limiter(
                self.log.debug, f"Listed {service_code} quotas"):
            paginator = self.aws_quotas.get_paginator(
                'list_service_quotas')
            quotas = {}
            paginate_args = {
                'ServiceCode': service_code,
                'PaginationConfig': {'PageSize': 100},
            }
            for page in paginator.paginate(**paginate_args):
                for quota in page['Quotas']:
                    quotas[quota['QuotaCode']] = quota['Value']
            return quotas

    def _listEC2Quotas(self):
        return self._listServiceQuotas('ec2')

    def _listEBSQuotas(self):
        return self._listServiceQuotas('ebs')

    def _listHosts(self):
        with self.non_mutating_rate_limiter(
                self.log.debug, "Listed hosts"):
            paginator = self.ec2_client.get_paginator('describe_hosts')
            hosts = []
            for page in paginator.paginate():
                hosts.extend(page['Hosts'])
            return hosts

    def _listInstances(self):
        with self.non_mutating_rate_limiter(
                self.log.debug, "Listed instances"):
            paginator = self.ec2_client.get_paginator('describe_instances')
            instances = []
            for page in paginator.paginate():
                for res in page['Reservations']:
                    instances.extend(res['Instances'])
            return instances

    def _listVolumes(self):
        with self.non_mutating_rate_limiter(
                self.log.debug, "Listed volumes"):
            paginator = self.ec2_client.get_paginator('describe_volumes')
            volumes = []
            for page in paginator.paginate():
                volumes.extend(page['Volumes'])
            return volumes

    def _listAmis(self):
        # Note: this is overridden in tests due to the filter
        with self.non_mutating_rate_limiter(
                self.log.debug, "Listed images"):
            paginator = self.ec2_client.get_paginator('describe_images')
            images = []
            for page in paginator.paginate(Owners=['self']):
                images.extend(page['Images'])
            return images

    def _listSnapshots(self):
        # Note: this is overridden in tests due to the filter
        with self.non_mutating_rate_limiter(
                self.log.debug, "Listed snapshots"):
            paginator = self.ec2_client.get_paginator('describe_snapshots')
            snapshots = []
            for page in paginator.paginate(OwnerIds=['self']):
                snapshots.extend(page['Snapshots'])
            return snapshots

    def _listObjects(self, bucket_name):
        if not bucket_name:
            return []

        bucket = self.s3.Bucket(bucket_name)
        with self.non_mutating_rate_limiter(
                self.log.debug, "Listed S3 objects"):
            return list(bucket.objects.all())

    def _listLaunchTemplates(self):
        name_filter = {
            'Name': 'launch-template-name',
            'Values': [f'{self.LAUNCH_TEMPLATE_PREFIX}-*'],
        }
        with self.non_mutating_rate_limiter:
            paginator = self.ec2_client.get_paginator(
                'describe_launch_templates')
            templates = []
            for page in paginator.paginate(Filters=[name_filter]):
                templates.extend(page['LaunchTemplates'])
            return templates

    def _getLatestImageIdByFilters(self, image_filters):
        # Normally we would decorate this method, but our cache key is
        # complex, so we serialize it to JSON and manage the cache
        # ourselves.
        cache_key = json.dumps(image_filters)
        val = self.image_id_by_filter_cache.get(cache_key)
        if val:
            return val

        with self.non_mutating_rate_limiter:
            res = list(self.ec2_client.describe_images(
                Filters=[
                    {k.capitalize(): v for k, v in fltr.items()}
                    for fltr in image_filters
                ]
            ).get("Images"))

        images = sorted(
            res,
            key=lambda k: k["CreationDate"],
            reverse=True
        )

        if not images:
            raise Exception(
                "No cloud-image (AMI) matches supplied image filters")
        else:
            val = images[0].get("ImageId")
            self.image_id_by_filter_cache[cache_key] = val
            return val

    def _getImageId(self, image):
        image_id = image.image_id
        image_filters = image.image_filters

        if image_filters is not None:
            return self._getLatestImageIdByFilters(image_filters)

        return image_id

    # This method is wrapped with an LRU cache in the constructor.
    def _getImage(self, image_id):
        with self.non_mutating_rate_limiter:
            resp = self.ec2_client.describe_images(ImageIds=[image_id])
            return resp['Images'][0]

    def _submitAllocateHost(self, label, flavor,
                            tags, hostname, log):
        return self.create_executor.submit(
            self._allocateHost,
            label, flavor,
            tags, hostname, log)

    def _completeAllocateHost(self, future):
        if not future.done():
            return None
        try:
            return future.result()
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'HostLimitExceeded':
                # Re-raise as a quota exception so that the
                # statemachine driver resets quota.
                raise exceptions.QuotaException(str(error))
            if (error.response['Error']['Code'] ==
                'InsufficientInstanceCapacity'):
                # Re-raise as CapacityException so it would have
                # "error.capacity" statsd_key, which can be handled
                # differently than "error.unknown"
                raise exceptions.CapacityException(str(error))
            raise

    def _allocateHost(self, label, flavor,
                      tags, hostname, log):
        args = dict(
            AutoPlacement='off',
            AvailabilityZone=label.az,
            InstanceType=flavor.instance_type,
            Quantity=1,
            HostRecovery='off',
            HostMaintenance='off',
            TagSpecifications=[
                {
                    'ResourceType': 'dedicated-host',
                    'Tags': tag_dict_to_list(tags),
                },
            ]
        )

        with self.rate_limiter(log.debug, "Allocated host"):
            log.debug("Allocating host %s", hostname)
            resp = self.ec2_client.allocate_hosts(**args)
            host_ids = resp['HostIds']
            log.debug("Allocated host %s as host %s",
                      hostname, host_ids[0])
            return dict(HostId=host_ids[0],
                        State='pending')

    def _submitCreateInstance(self, provider, label, flavor, image,
                              image_external_id, tags, hostname,
                              dedicated_host_id, launch_template_name, log):
        return self.create_executor.submit(
            self._createInstance,
            provider, label, flavor, image, image_external_id,
            tags, hostname, dedicated_host_id, launch_template_name, log)

    def _completeCreateInstance(self, future):
        if not future.done():
            return None
        try:
            return future.result()
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'VolumeLimitExceeded':
                # Re-raise as a quota exception so that the
                # statemachine driver resets quota.
                raise exceptions.QuotaException(str(error))
            if (error.response['Error']['Code'] ==
                'InsufficientInstanceCapacity'):
                # Re-raise as CapacityException so it would have
                # "error.capacity" statsd_key, which can be handled
                # differently than "error.unknown"
                raise exceptions.CapacityException(str(error))
            raise

    def _createInstance(self, provider, label, flavor, image,
                        image_external_id, tags, hostname,
                        dedicated_host_id, launch_template_name, log):
        if image_external_id:
            image_id = image_external_id
        else:
            image_id = self._getImageId(image)

        if flavor.fleet:
            return self._createFleet(provider, label, flavor,
                                     image_id, tags, hostname,
                                     launch_template_name, log)
        else:
            return self._runInstance(label, flavor, image_id, tags,
                                     hostname, dedicated_host_id, log)

    def _createLaunchTemplates(self, provider):
        fleet_labels = []
        for label_name, label in provider.labels.items():
            flavor = provider.flavors[label.flavor]
            # Create launch templates only for labels which use fleet
            if not flavor.fleet:
                continue
            fleet_labels.append((label, flavor))

        self.log.info("Creating launch templates")
        # Include the provider name here so that identical templates
        # used by different providers get different hashes (tags are
        # included in the hash).  This way each provider can be sure
        # it's managing its own templates.
        tags = {
            'zuul_system_id': self.system_id,
            'zuul_provider_name': provider.canonical_name,
        }
        existing_templates = dict()  # to avoid creation attempt
        created_templates = set()  # to avoid creation attempt

        for template in self._listLaunchTemplates():
            existing_templates[template['LaunchTemplateName']] = template

        # To replace the provider->label->template dictionary on this
        # endpoint.
        label_template_names = {}

        created = False
        for label, flavor in fleet_labels:
            template_data = {
                'KeyName': label.key_name,
                'MaintenanceOptions': {
                    'AutoRecovery': 'disabled',
                },
            }

            if label.security_group_ids:
                template_data['SecurityGroupIds'] = label.security_group_ids

            if label.imds_http_tokens == 'required':
                template_data['MetadataOptions'] = {
                    'HttpTokens': 'required',
                    'HttpEndpoint': 'enabled',
                    'HttpProtocolIpv6': 'enabled',
                }
            elif label.imds_http_tokens == 'optional':
                template_data['MetadataOptions'] = {
                    'HttpTokens': 'optional',
                    'HttpEndpoint': 'enabled',
                    'HttpProtocolIpv6': 'enabled',
                }

            if flavor.nested_virtualization:
                template_data['CpuOptions'] = {
                    'NestedVirtualization': 'enabled'
                }

            if label.userdata:
                userdata_base64 = base64.b64encode(
                    label.userdata.encode('utf-8')).decode('utf-8')
                template_data['UserData'] = userdata_base64

            template_args = dict(
                LaunchTemplateData=template_data,
                TagSpecifications=[
                    {
                        'ResourceType': 'launch-template',
                        'Tags': tag_dict_to_list(tags),
                    },
                ]
            )

            template_name = self._getLaunchTemplateName(template_args)

            label_template_names[label.name] = template_name

            if (template_name in existing_templates or
                template_name in created_templates):
                self.log.debug(
                    'Launch template %s already exists', template_name)
                continue

            template_args['LaunchTemplateName'] = template_name
            self.log.debug('Creating launch template %s', template_name)
            try:
                self.ec2_client.create_launch_template(**template_args)
                created_templates.add(template_name)
                self.log.debug('Launch template %s created', template_name)
                created = True
            except botocore.exceptions.ClientError as e:
                if (e.response['Error']['Code'] ==
                    'InvalidLaunchTemplateName.AlreadyExistsException'):
                    self.log.debug(
                        'Launch template %s already created',
                        template_name)
                else:
                    raise e
            except Exception:
                self.log.exception(
                    'Could not create launch template %s', template_name)

        self.provider_label_template_names[provider.canonical_name] =\
            label_template_names
        if created:
            # Clear the cache so that if we run this method again for
            # a very similar provider, our newly created templates
            # will be in the list so we don't try to create them
            # again.
            self._listLaunchTemplates.cache_clear()

    def _deleteLaunchTemplate(self, template_name):
        with self.rate_limiter(self.log.debug, "Deleted launch template"):
            self.ec2_client.delete_launch_template(
                LaunchTemplateName=template_name)
        self.log.info("Deleted unused launch template: %s",
                      template_name)

    def _getLaunchTemplateName(self, args):
        hasher = hashlib.sha256()
        hasher.update(json.dumps(args, sort_keys=True).encode('utf8'))
        sha = hasher.hexdigest()
        return (f'{self.LAUNCH_TEMPLATE_PREFIX}-{sha}')

    def _createFleet(self, provider, label, flavor, image_id, tags,
                     hostname, launch_template_name, log):
        overrides = []

        instance_types = flavor.fleet.get('instance_types', [])
        priority = 0
        for instance_type in instance_types:
            ebs_settings = {
                'DeleteOnTermination': True,
            }
            if label.volume_size:
                ebs_settings['VolumeSize'] = label.volume_size
            if label.volume_type:
                ebs_settings['VolumeType'] = label.volume_type
            if label.iops:
                ebs_settings['Iops'] = label.iops
            if label.throughput:
                ebs_settings['Throughput'] = label.throughput
            if label.kms_key_id:
                ebs_settings['KmsKeyId'] = label.kms_key_id
                ebs_settings['Encrypted'] = True
            override_dict = {
                'ImageId': image_id,
                'InstanceType': instance_type,
                'BlockDeviceMappings': [
                    {
                        'DeviceName': '/dev/sda1',
                        'Ebs': ebs_settings,
                    },
                ],
            }
            if flavor.fleet['allocation_strategy'] in [
                'prioritized', 'capacity-optimized-prioritized']:
                override_dict['Priority'] = priority
                priority += 1

            # Duplicate overrides for each subnet
            if label.subnet_ids:
                for subnet_id in label.subnet_ids:
                    overrides.append(dict(
                        **override_dict,
                        SubnetId=subnet_id,
                    ))
            else:
                overrides.append(override_dict)

        if flavor.market_type == 'spot':
            capacity_type_option = {
                'SpotOptions': {
                    'AllocationStrategy': flavor.fleet['allocation_strategy'],
                },
                'TargetCapacitySpecification': {
                    'TotalTargetCapacity': 1,
                    'DefaultTargetCapacityType': 'spot',
                },
            }
        else:
            capacity_type_option = {
                'OnDemandOptions': {
                    'AllocationStrategy': flavor.fleet['allocation_strategy'],
                },
                'TargetCapacitySpecification': {
                    'TotalTargetCapacity': 1,
                    'DefaultTargetCapacityType': 'on-demand',
                },
            }

        args = {
            **capacity_type_option,
            'LaunchTemplateConfigs': [
                {
                    'LaunchTemplateSpecification': {
                        'LaunchTemplateName': launch_template_name,
                        'Version': '$Latest',
                    },
                    'Overrides': overrides,
                },
            ],
            'Type': 'instant',
            'TagSpecifications': [
                {
                    'ResourceType': 'instance',
                    'Tags': tag_dict_to_list(tags),
                },
                {
                    'ResourceType': 'volume',
                    'Tags': tag_dict_to_list(tags),
                },
            ],
        }

        with self.rate_limiter(log.debug, "Created fleet"):
            resp = self.ec2_client.create_fleet(**args)

            if resp['Instances']:
                instance_id = resp['Instances'][0]['InstanceIds'][0]
            else:
                if resp['Errors']:
                    error = resp['Errors'][0]
                    raise Exception("Couldn't create fleet instance because "
                                    "of %s: %s", error["ErrorCode"],
                                    error["ErrorMessage"])
                raise Exception("Couldn't create fleet instance because "
                                "empty instance list was returned")

            log.debug("Created VM %s as instance %s using EC2 Fleet API",
                      hostname, instance_id)

            # Only return instance id in creating state, the state machine will
            # refresh until the instance object is returned otherwise it can
            # happen that the instance does not exist yet due to the AWS
            # eventual consistency
            return {'InstanceId': instance_id, 'State': {'Name': 'creating'}}

    def _getInstanceConfiguration(self, label, flavor, image_id, tags,
                                  dedicated_host_id):
        args = dict(
            ImageId=image_id,
            MinCount=1,
            MaxCount=1,
            KeyName=label.key_name,
            EbsOptimized=flavor.ebs_optimized,
            InstanceType=flavor.instance_type,
            NetworkInterfaces=[{
                'AssociatePublicIpAddress': flavor.public_ipv4,
                'DeviceIndex': 0}],
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': tag_dict_to_list(tags),
                },
                {
                    'ResourceType': 'volume',
                    'Tags': tag_dict_to_list(tags),
                },
            ],
            MaintenanceOptions={
                'AutoRecovery': 'disabled',
            },
        )

        if label.security_group_ids:
            args['NetworkInterfaces'][0]['Groups'] = label.security_group_ids

        if label.subnet_ids:
            args['NetworkInterfaces'][0]['SubnetId'] = random.choice(
                label.subnet_ids)

        if flavor.public_ipv6:
            args['NetworkInterfaces'][0]['Ipv6AddressCount'] = 1

        if flavor.nested_virtualization:
            args['CpuOptions'] = {
                'NestedVirtualization': 'enabled'
            }

        if label.userdata:
            args['UserData'] = label.userdata

        if label.iam_instance_profile:
            if 'name' in label.iam_instance_profile:
                args['IamInstanceProfile'] = {
                    'Name': label.iam_instance_profile['name']
                }
            elif 'arn' in label.iam_instance_profile:
                args['IamInstanceProfile'] = {
                    'Arn': label.iam_instance_profile['arn']
                }

        # Default block device mapping parameters are embedded in AMIs.
        # We might need to supply our own mapping before lauching the instance.
        # We basically want to make sure DeleteOnTermination is true and be
        # able to set the volume type and size.
        aws_image = self._getImage(image_id)
        # TODO: Flavors can also influence whether or not the VM spawns with a
        # volume -- we basically need to ensure DeleteOnTermination is true.
        # However, leaked volume detection may mitigate this.
        if aws_image.get('BlockDeviceMappings'):
            bdm = aws_image['BlockDeviceMappings']
            mapping = copy.deepcopy(bdm[0])
            if 'Ebs' in mapping:
                mapping['Ebs']['DeleteOnTermination'] = True
                if label.volume_size:
                    mapping['Ebs']['VolumeSize'] = label.volume_size
                if label.volume_type:
                    mapping['Ebs']['VolumeType'] = label.volume_type
                if label.iops:
                    mapping['Ebs']['Iops'] = label.iops
                if label.throughput:
                    mapping['Ebs']['Throughput'] = label.throughput
                # If the AMI is a snapshot, we cannot supply an
                # "encrypted" parameter. See
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2/client/run_instances.html
                # However, this documentation contradicts the above
                # and suggests that it may be harmless:
                # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AMIEncryption.html
                if 'Encrypted' in mapping['Ebs']:
                    del mapping['Ebs']['Encrypted']
                if label.kms_key_id:
                    mapping['Ebs']['KmsKeyId'] = label.kms_key_id
                    # If KmsKeyId is specified, Encrypted must be
                    # true.  We assume the UserGuide is correct, and this
                    # howto also supports that:
                    # https://aws.amazon.com/blogs/security/how-to-share-encrypted-amis-across-accounts-to-launch-encrypted-ec2-instances/
                    mapping['Ebs']['Encrypted'] = True
                args['BlockDeviceMappings'] = [mapping]

        if flavor.market_type == 'spot':
            args['InstanceMarketOptions'] = {
                'MarketType': 'spot',
                'SpotOptions': {
                    'SpotInstanceType': 'one-time',
                    'InstanceInterruptionBehavior': 'terminate'
                }
            }

        if label.imds_http_tokens == 'required':
            args['MetadataOptions'] = {
                'HttpTokens': 'required',
                'HttpEndpoint': 'enabled',
                'HttpProtocolIpv6': 'enabled',
            }
        elif label.imds_http_tokens == 'optional':
            args['MetadataOptions'] = {
                'HttpTokens': 'optional',
                'HttpEndpoint': 'enabled',
                'HttpProtocolIpv6': 'enabled',
            }

        if dedicated_host_id:
            placement = args.setdefault('Placement', {})
            placement.update({
                'Tenancy': 'host',
                'HostId': dedicated_host_id,
                'Affinity': 'host',
            })

        if label.az:
            placement = args.setdefault('Placement', {})
            placement['AvailabilityZone'] = label.az
        return args

    def _runInstance(self, label, flavor, image_id, tags, hostname,
                     dedicated_host_id, log):

        args = self._getInstanceConfiguration(label, flavor, image_id, tags,
                                              dedicated_host_id)

        with self.rate_limiter(log.debug, "Created instance"):
            log.debug("Creating VM %s", hostname)
            resp = self.ec2_client.run_instances(**args)
            instances = resp['Instances']
            if dedicated_host_id:
                log.debug("Created VM %s as instance %s on host %s",
                          hostname, instances[0]['InstanceId'],
                          dedicated_host_id)
            else:
                log.debug("Created VM %s as instance %s",
                          hostname, instances[0]['InstanceId'])
            return instances[0]

    def _deleteThread(self):
        while self._running:
            try:
                self._deleteThreadInner()
            except Exception:
                self.log.exception("Error in delete thread:")
                time.sleep(5)

    @staticmethod
    def _getBatch(the_queue):
        records = []
        try:
            record = the_queue.get(block=True, timeout=0.1)
            if record is None:
                return records
            records.append(record)
        except queue.Empty:
            return []
        while True:
            try:
                record = the_queue.get(block=False)
                if record is None:
                    return records
                records.append(record)
            except queue.Empty:
                break
            # The terminate call has a limit of 1k, but AWS recommends
            # smaller batches.  We limit to 50 here.
            if len(records) >= 50:
                break
        return records

    def _deleteThreadInner(self):
        records = self._getBatch(self.delete_instance_queue)
        if records:
            ids = []
            for (del_id, log) in records:
                ids.append(del_id)
                log.debug("Deleting instance %s", del_id)
            count = len(ids)
            with self.rate_limiter(log.debug, f"Deleted {count} instances"):
                self.ec2_client.terminate_instances(
                    InstanceIds=ids, SkipOsShutdown=True)

        if not self._running:
            return
        records = self._getBatch(self.delete_host_queue)
        if records:
            ids = []
            for (del_id, log) in records:
                ids.append(del_id)
                log.debug("Releasing host %s", del_id)
            count = len(ids)
            with self.rate_limiter(log.debug, f"Released {count} hosts"):
                self.ec2_client.release_hosts(HostIds=ids)

    def _releaseHost(self, external_id, log=None, immediate=False):
        if log is None:
            log = self.log
        for host in self._listHosts():
            if host['HostId'] == external_id:
                break
        else:
            log.warning("Host not found when releasing %s", external_id)
            return None
        if immediate:
            with self.rate_limiter(log.debug, "Released host"):
                log.debug(f"Deleting host {external_id}")
                self.ec2_client.release_hosts(
                    HostIds=[host['HostId']])
        else:
            self.delete_host_queue.put((external_id, log))
        return host

    def _deleteInstance(self, external_id, log=None, immediate=False):
        if log is None:
            log = self.log
        for instance in self._listInstances():
            if instance['InstanceId'] == external_id:
                break
        else:
            log.warning("Instance not found when deleting %s", external_id)
            return None
        if immediate:
            with self.rate_limiter(log.debug, "Deleted instance"):
                log.debug(f"Deleting instance {external_id}")
                self.ec2_client.terminate_instances(
                    InstanceIds=[instance['InstanceId']], SkipOsShutdown=True)
        else:
            self.delete_instance_queue.put((external_id, log))
        return instance

    def _deleteVolume(self, external_id):
        for volume in self._listVolumes():
            if volume['VolumeId'] == external_id:
                break
        else:
            self.log.warning("Volume not found when deleting %s", external_id)
            return None
        with self.rate_limiter(self.log.debug, "Deleted volume"):
            self.log.debug(f"Deleting volume {external_id}")
            try:
                self.ec2_client.delete_volume(VolumeId=volume['VolumeId'])
            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'NotFound':
                    self.log.warning(
                        "Volume not found when deleting %s", external_id)
                    return None
        return volume

    def _deleteAmi(self, external_id):
        for ami in self._listAmis():
            if ami['ImageId'] == external_id:
                break
        else:
            self.log.warning("AMI not found when deleting %s", external_id)
            return None
        with self.rate_limiter:
            self.log.debug(f"Deleting AMI {external_id}")
            try:
                self.ec2_client.deregister_image(ImageId=ami['ImageId'])
            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'NotFound':
                    self.log.warning(
                        "AMI not found when deleting %s", external_id)
                    return None
        return ami

    def _deleteSnapshot(self, external_id):
        for snap in self._listSnapshots():
            if snap['SnapshotId'] == external_id:
                break
        else:
            self.log.warning("Snapshot not found when deleting %s",
                             external_id)
            return None
        with self.rate_limiter:
            self.log.debug(f"Deleting Snapshot {external_id}")
            try:
                self.ec2_client.delete_snapshot(SnapshotId=snap['SnapshotId'])
            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'NotFound':
                    self.log.warning(
                        "Snapshot not found when deleting %s", external_id)
                    return None
        return snap

    def _deleteObject(self, bucket_name, external_id):
        with self.rate_limiter:
            self.log.debug("Deleting object %s", external_id)
            self.s3.Object(bucket_name, external_id).delete()

    # This method is wrapped by a TTL cache in init
    def _getAccountId(self):
        with self.non_mutating_rate_limiter:
            return self.sts_client.get_caller_identity()['Account']

    def _createSnapshot(self, volume_id, tags):
        with self.rate_limiter:
            self.log.debug(f"Creating snapshot for volume {volume_id}")
            return self.ec2_client.create_snapshot(
                VolumeId=volume_id,
                TagSpecifications=[
                    {
                        'ResourceType': 'snapshot',
                        'Tags': tag_dict_to_list(tags),
                    },
                ],
            )
