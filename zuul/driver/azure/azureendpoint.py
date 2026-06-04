# Copyright 2021-2025 Acme Gating, LLC
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

import cachetools.func
import json
import logging
import math
import random
import string
from concurrent.futures import ThreadPoolExecutor

from . import azul

from zuul import exceptions
from zuul.driver.azure.azuremodel import AzureResource, AzureInstance
from zuul.driver.util import (
    ImageUploader,
    LazyExecutorTTLCache,
    RateLimiter,
)
from zuul.model import QuotaInformation
from zuul.provider import (
    BaseImageUploadJob,
    BaseProviderEndpoint,
    statemachine
)

CACHE_TTL = 10
MIB = 1024 ** 2


def quota_info_from_sku(sku, spot=False):
    if not sku:
        return QuotaInformation(instances=1)

    cores = 0
    spot_cores = 0
    ram = 0
    for cap in sku['capabilities']:
        if cap['name'] == 'vCPUs':
            if spot:
                spot_cores = int(cap['value'])
            else:
                cores = int(cap['value'])
        if cap['name'] == 'MemoryGB':
            ram = int(float(cap['value']) * 1024)
    return QuotaInformation(
        cores=cores,
        spot_cores=spot_cores,
        ram=ram,
        instances=1)


def generate_password():
    while True:
        chars = random.choices(string.ascii_lowercase +
                               string.ascii_uppercase +
                               string.digits,
                               k=64)
        if ((set(string.ascii_lowercase) & set(chars)) and
            (set(string.ascii_uppercase) & set(chars)) and
            (set(string.digits) & set(chars))):
            return ''.join(chars)


class AzureDeleteStateMachine(statemachine.StateMachine):
    VM_DELETING = 'deleting vm'

    def __init__(self, endpoint, node, log):
        self.log = log
        self.endpoint = endpoint
        self.node = node
        super().__init__(node.delete_state)
        self.vm = None

    def advance(self):
        if self.state == self.START:
            if self.vm is None:
                self.vm = self.endpoint._deleteVirtualMachine(
                    self.node.azure_vm_id)
            self.state = self.VM_DELETING

        if self.state == self.VM_DELETING:
            self.vm = self.endpoint._refreshDelete(self.vm)
            if self.vm is None:
                self.state = self.COMPLETE

        if self.state == self.COMPLETE:
            self.complete = True


class AzureCreateStateMachine(statemachine.StateMachine):
    VM_CREATING_SUBMIT = 'submit creating vm'
    VM_CREATING = 'creating vm'
    NIC_QUERY = 'querying nic'
    PIP_QUERY = 'querying pip'

    def __init__(self, provider, endpoint, node, hostname, label,
                 flavor, image, image_external_id, tags, log):
        self.log = log
        self.provider = provider
        self.resource_group = provider.resource_group
        self.endpoint = endpoint
        self.node = node
        self.tags = tags.copy()
        self.hostname = hostname
        self.label = label
        self.flavor = flavor
        self.image = image
        self.public_ipv4 = None
        self.public_ipv6 = None
        self.nic = None
        self.create_future = None
        super().__init__(node.create_state)
        self.attempts = node.create_state.get("attempts", 0)
        self.image_external_id = node.create_state.get(
            "image_external_id", image_external_id)
        if self.image_external_id:
            self.image_reference = {'id': self.image_external_id}
        else:
            self.image_reference = None
        self.subnet_id = None

        # Restore local objects
        self.node.quota = self.endpoint.getQuotaForLabel(
            self.label, self.flavor)

        if self.state == self.VM_CREATING_SUBMIT:
            for instance in self.endpoint.listInstances():
                if instance.metadata.get('zuul_node_uuid') == node.uuid:
                    self.node.azure_vm_id = instance.azure_vm_id
            if self.node.azure_vm_id:
                self.state = self.VM_CREATING

        self.vm = None
        if self.node.azure_vm_id:
            vm = dict(
                type='Microsoft.Compute/virtualMachines',
                id=node.azure_vm_id,
            )
            self.vm = self.endpoint._refresh(vm)

    def toDict(self):
        data = super().toDict()
        data.update(
            attempts=self.attempts,
            image_external_id=self.image_external_id,
        )
        return data

    def advance(self):
        if self.state == self.START:
            # Find an appropriate image if filters were provided
            if self.image_reference is None:
                self.image_reference =\
                    self.endpoint._getImageReferenceForImage(self.image)
            if self.subnet_id is None:
                self.subnet_id =\
                    self.endpoint._getSubnetIdForLabel(
                        self.resource_group, self.label)
            self.state = self.VM_CREATING_SUBMIT

        if self.state == self.VM_CREATING_SUBMIT:
            if not self.create_future:
                self.create_future = self.endpoint._submitApi(
                    self.endpoint._createVirtualMachine,
                    self.resource_group, self.label, self.flavor,
                    self.image, self.image_reference, self.subnet_id,
                    self.tags, self.hostname, self.log)

            self.vm = self.endpoint._completeApi(self.create_future)
            if self.vm is None:
                return
            self.node.azure_vm_id = self.vm['id']
            self.state = self.VM_CREATING

        if self.state == self.VM_CREATING:
            self.vm = self.endpoint._refresh(self.vm)
            if self.endpoint._succeeded(self.vm):
                self.nic = (self.vm['properties']['networkProfile']
                            ['networkInterfaces'][0]).copy()
                self.nic['type'] = 'Microsoft.Network/networkInterfaces'
                self.state = self.NIC_QUERY
            elif self.endpoint._failed(self.vm):
                raise exceptions.LaunchStatusException("VM in failed state")
            else:
                return

        if self.state == self.NIC_QUERY:
            self.nic = self.endpoint._refresh(self.nic, force=True)
            if self.endpoint._failed(self.nic):
                raise exceptions.LaunchStatusException("NIC in failed state")
            elif not self.endpoint._succeeded(self.nic):
                return
            all_found = True
            for ip_config_data in self.nic['properties']['ipConfigurations']:
                ip_config_prop = ip_config_data['properties']
                if 'privateIPAddress' not in ip_config_prop:
                    all_found = False
            if all_found:
                for ip_config in self.nic['properties']['ipConfigurations']:
                    if 'publicIPAddress' in ip_config['properties']:
                        public_ip = ip_config['properties']['publicIPAddress']
                        ip_version = (ip_config['properties']
                                      ['privateIPAddressVersion'])
                        if ip_version == 'IPv4':
                            self.public_ipv4 = public_ip
                            self.public_ipv4['type'] =\
                                'Microsoft.Network/publicIPAddresses'
                        if ip_version == 'IPv6':
                            self.public_ipv6 = public_ip
                            self.public_ipv6['type'] =\
                                'Microsoft.Network/publicIPAddresses'
                self.state = self.PIP_QUERY

        if self.state == self.PIP_QUERY:
            all_found = True
            if self.public_ipv4:
                self.public_ipv4 = self.endpoint._refresh(
                    self.public_ipv4, force=True)
                if 'ipAddress' not in self.public_ipv4['properties']:
                    all_found = False
            if self.public_ipv6:
                self.public_ipv6 = self.endpoint._refresh(
                    self.public_ipv6, force=True)
                if 'ipAddress' not in self.public_ipv6['properties']:
                    all_found = False
            if all_found:
                self._updateNode()
                self.state = self.COMPLETE

        if self.state == self.COMPLETE:
            self.complete = True

    def _updateNode(self):
        instance = AzureInstance(self.endpoint.region, self.vm,
                                 self.node.quota, self.nic,
                                 self.public_ipv4, self.public_ipv6)
        self.node.updateFromInstance(instance)


class AzureImageUploadJob(BaseImageUploadJob):
    def __init__(self, endpoint, resource_group,
                 provider_image, image_name,
                 image_format, metadata,
                 timeout):
        super().__init__()
        self.endpoint = endpoint
        self.resource_group = resource_group
        self.provider_image = provider_image
        self.image_name = image_name
        self.image_format = image_format
        self.metadata = metadata
        self.timeout = timeout

    def run(self, filename):
        self.endpoint.log.debug("Uploading image %s",
                                self.image_name)

        image_id = self.endpoint._uploadImageSnapshot(
            self.resource_group, self.provider_image, self.image_name,
            filename, self.image_format, self.metadata, self.timeout)
        return image_id


class AzureSnapshotUploader(ImageUploader):
    segment_size = 4 * MIB

    def __init__(self, resource_group, *args, **kw):
        super().__init__(*args, **kw)
        self.resource_group = resource_group

    def uploadSegment(self, segment):
        data = segment.data
        start = segment.offset
        end = start + len(data) - 1
        self.retry(
            self.endpoint._client.upload_sas_chunk,
            self.url, start, end, data
        )

    def startUpload(self):
        disk_info = {
            "location": self.endpoint.region,
            "tags": self.metadata,
            "properties": {
                "creationData": {
                    "createOption": "Upload",
                    "uploadSizeBytes": self.size,
                }
            }
        }
        self.log.debug("Creating disk for image upload")

        with self.endpoint.rate_limiter:
            r = self.endpoint._client.resource_group_disks.create(
                self.resource_group, self.image_name, disk_info)
        r = self.endpoint._client.wait_for_async_operation(r)

        if r['status'] != 'Succeeded':
            raise Exception("Unable to create disk for image upload")
        self.disk_id = r['properties']['output']['id']

        disk_grant = {
            "access": "Write",
            "durationInSeconds": 24 * 60 * 60,
        }
        self.log.debug("Enabling write access to disk for image upload")
        with self.endpoint.rate_limiter:
            r = self.endpoint._client.resource_group_disks.post(
                self.resource_group, self.image_name,
                'beginGetAccess', disk_grant)
        r = self.endpoint._client.wait_for_async_operation(r)

        if r['status'] != 'Succeeded':
            raise Exception("Unable to begin write access on disk")
        self.url = r['properties']['output']['accessSAS']
        self.log.debug("Uploading image")

    def finishUpload(self):
        disk_grant = {}
        self.log.debug("Disabling write access to disk for image upload")
        with self.endpoint.rate_limiter:
            r = self.endpoint._client.resource_group_disks.post(
                self.resource_group, self.image_name,
                'endGetAccess', disk_grant)
        r = self.endpoint._client.wait_for_async_operation(r)

        if r['status'] != 'Succeeded':
            raise Exception("Unable to end write access on disk")

        image_info = {
            "location": self.endpoint.region,
            "tags": self.metadata,
            "properties": {
                "hyperVGeneration": "V2",
                "storageProfile": {
                    "osDisk": {
                        "osType": "Linux",
                        "managedDisk": {
                            "id": self.disk_id,
                        },
                        "osState": "Generalized"
                    },
                    "zoneResilient": True
                }
            }
        }
        self.log.debug("Creating image from disk")
        with self.endpoint.rate_limiter:
            image = self.endpoint._client.resource_group_images.create(
                self.resource_group, self.image_name, image_info)
        image = self.endpoint._client.wait_for_async_operation(image)

        if image['status'] != 'Succeeded':
            raise Exception("Unable to create image from disk")

        self.log.debug("Deleting disk for image upload")
        with self.endpoint.rate_limiter:
            r = self.endpoint._client.resource_group_disks.delete(
                self.resource_group,
                self.image_name)
        r = self.endpoint._client.wait_for_async_operation(r)

        if r['status'] != 'Succeeded':
            raise Exception("Unable to delete disk for image upload")

        return image['properties']['output']['id']

    def abortUpload(self):
        try:
            self.finishUpload()
        except Exception:
            pass
        with self.endpoint.rate_limiter:
            self.endpoint._client.resource_group_disks.delete(
                self.resource_group,
                self.image_name)


class AzureProviderEndpoint(BaseProviderEndpoint):
    """An Azure Endpoint corresponds to a single Azure region, and can include
    multiple availability zones."""

    IMAGE_UPLOAD_SLEEP = 30

    def __init__(self, zk_client, driver, connection, region, system_id):
        name = f'{connection.connection_name}-{region}'
        super().__init__(zk_client, driver, connection, name, system_id)
        self.log = logging.getLogger(f"zuul.azure.{self.name}")
        self.region = region

        self.rate_limiter = RateLimiter(self.name,
                                        connection.rate)

        self.image_id_by_filter_cache = cachetools.TTLCache(
            maxsize=8192, ttl=(5 * 60))

    def _getClient(self):
        return azul.AzureCloud(
            subscription_id=self.connection.subscription_id,
            tenant_id=self.connection.tenant_id,
            client_id=self.connection.client_id,
            client_secret=self.connection.client_secret,
            federated_token_file=self.connection.federated_token_file,
        )

    def startEndpoint(self):
        self._running = True
        self.log.debug("Starting Azure endpoint")
        self._client = self._getClient()

        # The default http connection pool size is 10; match it for
        # efficiency.
        workers = 10
        self.log.info("Create executor with max workers=%s", workers)
        self.api_executor = ThreadPoolExecutor(
            thread_name_prefix=f'azure-api-{self.name}',
            max_workers=workers)

        # Use a lazy TTL cache for these.  This uses the TPE to
        # asynchronously update the cached values, meanwhile returning
        # the previous cached data if available.  This means every
        # call after the first one is instantaneous.
        self._listPublicIPAddresses = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(
                self._listPublicIPAddresses)
        self._listNetworkInterfaces = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(
                self._listNetworkInterfaces)
        self._listVirtualMachines = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(
                self._listVirtualMachines)
        self._listDisks = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(
                self._listDisks)
        self._listImages = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(
                self._listImages)

        self.skus = {}
        self._getSKUs()

    def stopEndpoint(self):
        self.log.debug("Stopping Azure endpoint")
        self.api_executor.shutdown()
        self._running = False

    def listResources(self, providers):
        resource_groups = set()
        for provider in providers:
            resource_groups.add(provider.resource_group)

        for vm in self._listVirtualMachines():
            yield AzureResource(vm.get('tags', {}),
                                AzureResource.TYPE_INSTANCE,
                                vm['id'])
        for nic in self._listNetworkInterfaces():
            yield AzureResource(nic.get('tags', {}),
                                AzureResource.TYPE_NIC, nic['id'])
        for pip in self._listPublicIPAddresses():
            yield AzureResource(pip.get('tags', {}),
                                AzureResource.TYPE_PIP, pip['id'])
        for disk in self._listDisks():
            yield AzureResource(disk.get('tags', {}),
                                AzureResource.TYPE_DISK, disk['id'])
        for image in self._listImages():
            yield AzureResource(image.get('tags', {}),
                                AzureResource.TYPE_IMAGE, image['id'])

    def deleteResource(self, resource):
        self.log.info("Deleting leaked %s: %s",
                      resource.type, resource.id)
        if resource.type == AzureResource.TYPE_INSTANCE:
            crud = self._client.virtual_machines
        elif resource.type == AzureResource.TYPE_NIC:
            crud = self._client.network_interfaces
        elif resource.type == AzureResource.TYPE_PIP:
            crud = self._client.public_ip_addresses
        elif resource.type == AzureResource.TYPE_DISK:
            crud = self._client.disks
        elif resource.type == AzureResource.TYPE_IMAGE:
            crud = self._client.images
        with self.rate_limiter:
            try:
                crud.delete_by_id(resource.id)
            except azul.AzureNotFoundError:
                pass

    def listInstances(self):
        for vm in self._listVirtualMachines():
            sku = self.skus.get((vm['properties']['hardwareProfile']['vmSize'],
                                 vm['location']))
            spot = vm.get('properties', {}).get('priority', '') == 'Spot'
            quota = quota_info_from_sku(sku, spot)
            yield AzureInstance(self.region, vm, quota)

    def getQuotaForLabel(self, label, flavor):
        # This should not rely on the client connection, only the
        # quota cache.
        check_vm_size = flavor.vm_size
        key = f'vm-{flavor.priority}-{check_vm_size}'
        quota = self.quota_cache.getResource(key)
        return quota

    def refreshQuotaLimits(self, update):
        if self.quota_cache.hasLimits() and not update:
            return False

        with self.rate_limiter:
            r = self._client.compute_usages.list(self.region)
        cores = spot_cores = instances = math.inf
        for item in r:
            if item['name']['value'] == 'cores':
                cores = item['limit']
            if item['name']['value'] == 'lowPriorityCores':
                spot_cores = item['limit']
            elif item['name']['value'] == 'virtualMachines':
                instances = item['limit']
        limits = QuotaInformation(cores=cores,
                                  spot_cores=spot_cores,
                                  instances=instances,
                                  default=math.inf)
        self.quota_cache.setLimits(limits)
        return True

    def refreshQuotaForLabel(self, label, flavor, update):
        key = f'vm-{flavor.priority}-{flavor.vm_size}'
        if update or not self.quota_cache.hasResource(key):
            sku = self.skus.get((flavor.vm_size, self.region))
            spot = flavor.priority == 'spot'
            quota = quota_info_from_sku(sku, spot)
            self.quota_cache.setResource(key, quota)

    def getImageUploadJob(self, resource_group, provider_image,
                          image_name, image_format, metadata, md5,
                          sha256):
        timeout = provider_image.import_timeout
        return AzureImageUploadJob(
            self, resource_group,
            provider_image, image_name,
            image_format, metadata,
            timeout)

    def _uploadImageSnapshot(self, resource_group, provider_image,
                             image_name, filename, image_format,
                             metadata, timeout):
        # Import snapshot
        uploader = AzureSnapshotUploader(
            resource_group, self, self.log, filename,
            image_name, metadata)
        self.log.debug("Uploading image %s", image_name)
        image_id = uploader.upload(timeout)

        self.log.debug("Upload of image %s complete as %s",
                       image_name, image_id)
        return image_id

    # Local implementation below

    def _submitApi(self, api, *args, **kw):
        return self.api_executor.submit(
            api, *args, **kw)

    def _completeApi(self, future):
        if not future.done():
            return None
        return future.result()

    @staticmethod
    def _succeeded(obj):
        return obj.get('properties', {}).get(
            'provisioningState') == 'Succeeded'

    @staticmethod
    def _failed(obj):
        return obj.get('properties', {}).get(
            'provisioningState') == 'Failed'

    def _refresh(self, obj, force=False):
        if self._succeeded(obj) and not force:
            return obj

        if obj['type'] == 'Microsoft.Network/publicIPAddresses':
            l = self._listPublicIPAddresses()
        if obj['type'] == 'Microsoft.Network/networkInterfaces':
            l = self._listNetworkInterfaces()
        if obj['type'] == 'Microsoft.Compute/virtualMachines':
            l = self._listVirtualMachines()

        for new_obj in l:
            if new_obj['id'].lower() == obj['id'].lower():
                return new_obj
        return obj

    def _refreshDelete(self, obj):
        if obj is None:
            return obj

        if obj['type'] == 'Microsoft.Network/publicIPAddresses':
            l = self._listPublicIPAddresses()
        if obj['type'] == 'Microsoft.Network/networkInterfaces':
            l = self._listNetworkInterfaces()
        if obj['type'] == 'Microsoft.Compute/virtualMachines':
            l = self._listVirtualMachines()

        for new_obj in l:
            if new_obj['id'].lower() == obj['id'].lower():
                return new_obj
        return None

    def _getSKUs(self):
        self.log.debug("Querying compute SKUs")
        with self.rate_limiter:
            for sku in self._client.compute_skus.list():
                for location in sku['locations']:
                    key = (sku['name'], location)
                    self.skus[key] = sku
        self.log.debug("Done querying compute SKUs")

    def _listPublicIPAddresses(self):
        with self.rate_limiter:
            return self._client.public_ip_addresses.list()

    def _listNetworkInterfaces(self):
        with self.rate_limiter:
            return self._client.network_interfaces.list()

    def _listVirtualMachines(self):
        with self.rate_limiter:
            return self._client.virtual_machines.list()

    def _createVirtualMachine(self, resource_group, label, flavor,
                              image, image_reference, subnet_id, tags,
                              hostname, log):
        os_profile = {'computerName': hostname}
        if label.key_data:
            linux_config = {
                'ssh': {
                    'publicKeys': [{
                        'path': "/home/%s/.ssh/authorized_keys" % (
                            image.username),
                        'keyData': label.key_data,
                    }]
                },
                "disablePasswordAuthentication": True,
            }
            os_profile['linuxConfiguration'] = linux_config
        if image.username:
            os_profile['adminUsername'] = image.username
        if image.generate_password:
            os_profile['adminPassword'] = generate_password()

        # Nodepool provided a plaintext password option which doesn't
        # make sense in NIZ.  It also supported the "customData"
        # attribute but Azure recommends "userData" instead, and that
        # matches other clouds, so we only support that.

        # Begin disk config

        os_disk = {
            'createOption': 'FromImage',
            'deleteOption': 'Delete',
        }
        if label.volume_size:
            os_disk['diskSizeGB'] = label.volume_size

        if label.ephemeral_disk:
            # Caching must be set to ReadOnly for ephemeral disks
            # https://learn.microsoft.com/en-us/azure/virtual-machines/ephemeral-os-disks-deploy
            os_disk['caching'] = 'ReadOnly'
            os_disk['diffDiskSettings'] = {
                # This causes the disk to be ephemeral
                'option': 'Local',
                # Another frequently used option is "placement", but
                # if it is omitted, Azure will pick the correct one
                # for the VM based on the SKU.
                # https://learn.microsoft.com/en-us/azure/virtual-machines/ephemeral-os-disks#placement-options-for-ephemeral-os-disks
            }

        # Begin network config
        # There are two parameters for IP addresses: SKU and
        # allocation method.  SKU is "basic" or "standard".
        # Allocation method is "static" or "dynamic".  Between IPv4
        # and v6, SKUs cannot be mixed (the same sku must be used for
        # both protocols).  The standard SKU only supports static
        # allocation.  Static is cheaper than dynamic, but basic is
        # cheaper than standard.  Also, dynamic is faster than static.
        # Basic is being retired, and static allocation (contrary to
        # what it sounds like) works for our use case.
        # Therefore, we use Standird + static for everything.

        def make_ip_config(name, version, subnet_id, pip):
            ip_config = {
                'name': f'{name}-config',
                'properties': {
                    'privateIPAddressVersion': version,
                    'subnet': {
                        'id': subnet_id
                    },
                }
            }
            if pip:
                ip_config['properties']['publicIPAddressConfiguration'] = {
                    'name': f'{name}-ip',
                    'tags': tags,
                    'properties': {
                        'deleteOption': 'Delete',
                        'publicIPAllocationMethod': 'Static',
                        'publicIPAddressVersion': version,
                    },
                    'sku': {
                        'name': 'Standard',
                    },
                }
            return ip_config

        ip_configs = []

        ipv6 = flavor.ipv6 or flavor.public_ipv6
        ipv4 = flavor.ipv4 or flavor.public_ipv4 or not ipv6

        if ipv4:
            ip_configs.append(make_ip_config(f'{hostname}-v4',
                                             'IPv4', subnet_id,
                                             flavor.public_ipv4))
        if ipv6:
            ip_configs.append(make_ip_config(f'{hostname}-v6',
                                             'IPv6', subnet_id,
                                             flavor.public_ipv6))

        nic_config = {
            'name': f'{hostname}-nic',
            'tags': tags,
            'properties': {
                'deleteOption': 'Delete',
                'ipConfigurations': ip_configs
            }
        }

        spec = {
            'location': self.region,
            'tags': tags,
            'properties': {
                'osProfile': os_profile,
                'hardwareProfile': {
                    'vmSize': flavor.vm_size,
                },
                'storageProfile': {
                    'imageReference': image_reference,
                    'osDisk': os_disk,
                },
                'networkProfile': {
                    'networkApiVersion': (
                        self._client.network_interfaces.args['apiVersion']),
                    'networkInterfaceConfigurations': [nic_config],
                },
            },
        }
        if label.userdata:
            spec['properties']['userData'] = label.userdata

        # build resource id for all configured User-assigned Identities
        uai_resource_ids = set()
        for uai in label.user_assigned_identities:
            uai_rg_name = uai.get("resource_group", resource_group)
            uai_id = self._client.resource_group_managed_identities.id(
                resourceGroupName=uai_rg_name, resourceName=uai['name'])
            uai_resource_ids.add(uai_id)

        # adding empty userAssignedIdentities is not allowed by Azure
        if uai_resource_ids:
            spec['identity'] = {
                'type': 'UserAssigned',
                'userAssignedIdentities': {
                    rid: {} for rid in uai_resource_ids
                },
            }

        if flavor.priority == 'spot':
            spec['properties']['evictionPolicy'] = 'Delete'
            spec['properties']['priority'] = 'Spot'

        with self.rate_limiter:
            log.debug(f"Creating VM {hostname}")
            return self._client.resource_group_virtual_machines.create(
                resource_group, hostname, spec)

    def _deleteVirtualMachine(self, vm_id):
        vm_id_cmp = vm_id.lower() if vm_id else None
        for vm in self._listVirtualMachines():
            if vm['id'].lower() == vm_id_cmp:
                break
        else:
            self.log.warning("VM not found when deleting %s", vm_id)
            return None
        with self.rate_limiter:
            self.log.debug("Deleting VM %s", vm_id)
            self._client.virtual_machines.delete_by_id(vm_id)
        return vm

    def _listDisks(self):
        with self.rate_limiter:
            return self._client.disks.list()

    def _listImages(self):
        with self.rate_limiter:
            return self._client.images.list()

    def _getImageReferenceFromFilter(self, image_filter):
        # Normally we would decorate this method, but our cache key is
        # complex, so we serialize it to JSON and manage the cache
        # ourselves.
        cache_key = json.dumps(image_filter)
        if val := self.image_id_by_filter_cache.get(cache_key):
            return val

        images = self._listImages()
        images = [i for i in images
                  if i['properties']['provisioningState'] == 'Succeeded']
        if image_filter['name']:
            images = [i for i in images
                      if i['name'] == image_filter['name']]
        if image_filter['location']:
            images = [i for i in images
                      if i['location'] == image_filter['location']]
        if image_filter['tags']:
            for k, v in image_filter['tags'].items():
                images = [i for i in images if i['tags'].get(k) == v]
        images = sorted(images, key=lambda i: i['name'])
        if not images:
            raise Exception("Unable to find image matching filter: %s",
                            image_filter)
        image = images[-1]
        self.log.debug("Found image matching filter: %s", image)
        return {'id': image['id']}

    def _getImageReferenceForImage(self, image):
        if image.image_id:
            return {'id': image.image_id}
        if image.image_reference:
            return image.image_reference
        if image.image_filter:
            return self._getImageReferenceFromFilter(image.image_filter)
        if g := image.community_gallery_image:
            gallery_image_id = (
                f"/CommunityGalleries/{g['gallery_name']}"
                f"/Images/{g['name']}"
            )
            if 'version' in g:
                gallery_image_id += f"/Versions/{g['version']}"
            image_reference = {
                'communityGalleryImageId': gallery_image_id
            }
            return image_reference
        if g := image.shared_gallery_image:
            gallery_image_id = (
                f"/SharedGalleries/{g['gallery_name']}"
                f"/Images/{g['name']}"
            )
            if 'version' in g:
                gallery_image_id += f"/Versions/{g['version']}"
            image_reference = {
                'sharedGalleryImageId': gallery_image_id
            }
            return image_reference

    def _getSubnetIdForLabel(self, resource_group, label):
        if label.subnet_id:
            return label.subnet_id

        ref = label.subnet_reference
        subnet_id = self._client.subnets.id(
            resourceGroupName=ref.get('resource_group', resource_group),
            virtualNetworkName=ref['network'],
            resourceName=ref['subnet'],
        )
        return subnet_id
