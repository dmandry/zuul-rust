# Copyright 2024 BMW Group
# Copyright 2024 Acme Gating, LLC
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

from enum import StrEnum

from zuul import model
from zuul.provider import statemachine
from zuul.driver.aws.util import tag_list_to_dict


class AwsProviderNode(model.ProviderNode, subclass_id="aws"):
    def __init__(self):
        super().__init__()
        self._set(
            aws_instance_id=None,
            aws_dedicated_host_id=None,
            aws_launch_template_name=None,
        )

    def getDriverData(self):
        return dict(
            aws_instance_id=self.aws_instance_id,
            aws_dedicated_host_id=self.aws_dedicated_host_id,
            aws_launch_template_name=self.aws_launch_template_name,
        )

    def getRequiredResources(self):
        if self.aws_launch_template_name:
            return [self.aws_launch_template_name]
        return []


class AwsInstance(statemachine.Instance):
    def __init__(self, region, instance, host, quota):
        super().__init__()
        self.aws_instance_id = instance and instance['InstanceId'] or None
        self.aws_dedicated_host_id = host and host['HostId'] or None
        self.metadata = tag_list_to_dict(instance.get('Tags'))
        self.private_ipv4 = instance.get('PrivateIpAddress')
        self.private_ipv6 = None
        self.public_ipv4 = instance.get('PublicIpAddress')
        self.public_ipv6 = None
        self.cloud = 'AWS'
        self.region = region
        self.az = None
        self.quota = quota

        self.az = instance.get('Placement', {}).get('AvailabilityZone')

        for iface in instance.get('NetworkInterfaces', [])[:1]:
            if iface.get('Ipv6Addresses'):
                v6addr = iface['Ipv6Addresses'][0]
                self.public_ipv6 = v6addr.get('Ipv6Address')
        self.interface_ip = (self.public_ipv4 or self.public_ipv6 or
                             self.private_ipv4 or self.private_ipv6)

    def getQuotaInformation(self):
        return self.quota

    @property
    def external_id(self):
        if self.aws_dedicated_host_id:
            return (f'instance={self.aws_instance_id} '
                    f'host={self.aws_dedicated_host_id}')
        return f'instance={self.aws_instance_id}'


class AwsResource(statemachine.Resource):
    class ResourceType(StrEnum):
        HOST = 'host'
        INSTANCE = 'instance'
        AMI = 'ami'
        SNAPSHOT = 'snapshot'
        VOLUME = 'volume'
        OBJECT = 'object'
        LAUNCH_TEMPLATE = 'launch-template'

    def __init__(self, metadata, type, id, bucket_name=None):
        super().__init__(metadata, type)
        self.id = id
        self.bucket_name = bucket_name

    @property
    def unique_id(self):
        return '-'.join([self.type, self.id])
