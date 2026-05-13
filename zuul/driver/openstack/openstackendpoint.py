# Copyright (C) 2011-2013 OpenStack Foundation
# Copyright 2017 Red Hat
# Copyright 2022 Acme Gating, LLC
# Copyright 2022-2024 Acme Gating, LLC
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

import functools
import logging
import math
import operator
import time
import urllib.parse
from collections import UserDict
from concurrent.futures import ThreadPoolExecutor

import openstack
from keystoneauth1.exceptions.catalog import EndpointNotFound

from zuul import exceptions
from zuul.driver.openstack.openstackmodel import (
    OpenstackResource,
    OpenstackInstance,
)
from zuul.driver.util import (
    LazyExecutorTTLCache,
    RateLimiter,
    Timer,
)
from zuul.model import QuotaInformation
from zuul.provider import (
    BaseImageUploadJob,
    BaseProviderEndpoint,
    statemachine
)


CACHE_TTL = 10


def quota_from_flavor(flavor, label=None, volumes=None):
    args = dict(instances=1,
                cores=flavor.vcpus,
                ram=flavor.ram)
    if label and label.boot_from_volume:
        args['volumes'] = 1
        args['volume-gb'] = label.volume_size
    elif volumes:
        args['volumes'] = len(volumes)
        args['volume-gb'] = sum([v.size for v in volumes])
    return QuotaInformation(**args)


def quota_from_limits(compute, volume):
    def bound_value(value):
        if value == -1:
            return math.inf
        return value

    args = dict(
        instances=bound_value(compute.max_total_instances),
        cores=bound_value(compute.max_total_cores),
        ram=bound_value(compute.max_total_ram_size))
    if volume is not None:
        args['volumes'] = bound_value(volume['absolute']['maxTotalVolumes'])
        args['volume-gb'] = bound_value(
            volume['absolute']['maxTotalVolumeGigabytes'])
    return QuotaInformation(**args)


def is_message_about_quota(msg):
    msg = msg.lower()
    if 'quota' in msg:
        return True
    if 'ports exceeded' in msg:
        return True
    if all(s in msg for s in ('insufficient', 'resources')):
        # Exceeded maximum number of retries. Exceeded max scheduling
        # attempts 6 for instance
        # f9daa9f2-1181-4693-827f-ba84cd4b002c. Last exception:
        # Insufficient compute resources: Free vcpu 0.00 VCPU <
        # requested 8 VCPU.
        return True


class ZuulOpenstackServer(UserDict):
    # Most of OpenStackSDK is designed around a dictionary interface,
    # but due to the historic use of the Munch object, some code
    # (add_server_interfaces) accesses values by attribute instead of
    # key.  For just those values, we provide getters and setters.

    @property
    def access_ipv4(self):
        return self.data.get('access_ipv4')

    @access_ipv4.setter
    def access_ipv4(self, value):
        self.data['access_ipv4'] = value

    @property
    def public_v4(self):
        return self.data.get('public_v4')

    @public_v4.setter
    def public_v4(self, value):
        self.data['public_v4'] = value

    @property
    def private_v4(self):
        return self.data.get('private_v4')

    @private_v4.setter
    def private_v4(self, value):
        self.data['private_v4'] = value

    @property
    def access_ipv6(self):
        return self.data.get('access_ipv6')

    @access_ipv6.setter
    def access_ipv6(self, value):
        self.data['access_ipv6'] = value

    @property
    def public_v6(self):
        return self.data.get('public_v6')

    @public_v6.setter
    def public_v6(self, value):
        self.data['public_v6'] = value


class OpenstackDeleteStateMachine(statemachine.StateMachine):
    FLOATING_IP_DELETING = 'deleting floating ip'
    SERVER_DELETE = 'delete server'
    SERVER_DELETING = 'deleting server'

    def __init__(self, endpoint, node, log):
        self.log = log
        self.endpoint = endpoint
        self.node = node
        super().__init__(node.delete_state)
        self.delete_future = None

        # Restore local objects
        self.server = None
        self.floating_ips = []
        if self.node.openstack_server_id:
            self.server = self.endpoint._getServer(
                self.node.openstack_server_id)
            if (self.server and
                self.endpoint._hasFloatingIps() and
                self.server.get('addresses')):
                self.floating_ips = self.endpoint._getFloatingIps(
                    self.server)

    def advance(self):
        if self.state == self.START:
            if self.node.openstack_server_id:
                for fip in self.floating_ips:
                    self.endpoint._deleteFloatingIp(fip)
                    self.state = self.FLOATING_IP_DELETING
                if not self.floating_ips:
                    self.state = self.SERVER_DELETE
            else:
                self.state = self.COMPLETE

        if self.state == self.FLOATING_IP_DELETING:
            fips = []
            for fip in self.floating_ips:
                fip = self.endpoint._refreshFloatingIpDelete(fip)
                if not fip or fip['status'] == 'DOWN':
                    fip = None
                if fip:
                    fips.append(fip)
            self.floating_ips = fips
            if self.floating_ips:
                return
            else:
                self.state = self.SERVER_DELETE

        if self.state == self.SERVER_DELETE:
            if not self.delete_future:
                self.delete_future = self.endpoint._submitApi(
                    self.endpoint._deleteServer,
                    self.node.openstack_server_id)
            if self.endpoint._completeApi(self.delete_future):
                self.state = self.SERVER_DELETING

        if self.state == self.SERVER_DELETING:
            self.server = self.endpoint._refreshServerDelete(self.server)
            if self.server:
                return
            else:
                self.state = self.COMPLETE

        if self.state == self.COMPLETE:
            self.complete = True


class OpenstackCreateStateMachine(statemachine.StateMachine):
    SERVER_CREATING_SUBMIT = 'submit creating server'
    SERVER_CREATING = 'creating server'
    FLOATING_IP_CREATING = 'creating floating ip'
    FLOATING_IP_ATTACHING = 'attaching floating ip'

    def __init__(self, endpoint, node, hostname, label, flavor, image,
                 image_external_id, tags, log):
        self.log = log
        self.endpoint = endpoint
        self.node = node
        self.label = label
        self.flavor = flavor
        self.image = image
        self.hostname = hostname
        self.az = label.az
        self.create_future = None
        super().__init__(node.create_state)
        self.attempts = node.create_state.get("attempts", 0)
        self.image_external_id = node.create_state.get(
            "image_external_id", image_external_id)
        self.config_drive = image.config_drive

        if image_external_id:
            self.image_external = image_external_id
        else:
            # launch using unmanaged cloud image

            if image.image_id:
                # Using a dict with the ID bypasses an image search during
                # server creation.
                self.image_external = dict(id=image.image_id)
            else:
                self.image_external = image.external_name

        meta = {}
        meta.update(tags)
        self.metadata = meta
        self.os_flavor = self.endpoint._findFlavor(
            flavor_name=flavor.flavor_name,
        )
        self.node.quota = quota_from_flavor(self.os_flavor, label=self.label)

        if self.state == self.SERVER_CREATING_SUBMIT:
            for instance in self.endpoint.listInstances():
                if instance.metadata.get('zuul_node_uuid') == node.uuid:
                    self.node.openstack_server_id =\
                        instance.openstack_server_id
            if self.node.openstack_server_id:
                self.state = self.SERVER_CREATING

        self.server = None
        if self.node.openstack_server_id:
            self.server = self.endpoint._refreshServer(
                dict(
                    id=self.node.openstack_server_id,
                    status='_unknown',
                ))
        self.floating_ip = None
        if self.node.openstack_floating_ip_id:
            self.floating_ip = self.endpoint._refreshFloatingIp(
                dict(id=self.node.openstack_floating_ip_id))

    def _handleServerFault(self):
        # Return True if this is a quota fault
        if not self.node.openstack_server_id:
            return
        try:
            server = self.endpoint._getServerByIdNow(
                self.node.openstack_server_id)
            if not server:
                return
            fault = server.get('fault', {}).get('message')
            if fault:
                self.log.error('Detailed node error: %s', fault)
                if is_message_about_quota(fault):
                    return True
        except Exception:
            self.log.exception(
                'Failed to retrieve node error information:')

    def toDict(self):
        data = super().toDict()
        data.update(
            attempts=self.attempts,
            image_external_id=self.image_external_id,
        )
        return data

    def advance(self):
        if self.state == self.START:
            self.state = self.SERVER_CREATING_SUBMIT

        if self.state == self.SERVER_CREATING_SUBMIT:
            if not self.create_future:
                self.create_future = self.endpoint._submitApi(
                    self.endpoint._createServer,
                    self.hostname,
                    image=self.image_external,
                    flavor=self.os_flavor,
                    key_name=self.label.key_name,
                    az=self.az,
                    config_drive=self.config_drive,
                    networks=self.label.networks,
                    security_groups=self.label.security_groups,
                    boot_from_volume=self.label.boot_from_volume,
                    volume_size=self.label.volume_size,
                    instance_properties=self.metadata,
                    userdata=self.label.userdata,
                )
            try:
                try:
                    self.server = self.endpoint._completeApi(
                        self.create_future)
                    if self.server is None:
                        return
                    self.node.openstack_server_id = self.server['id']
                    self.state = self.SERVER_CREATING
                except openstack.cloud.exc.OpenStackCloudCreateException as e:
                    if e.resource_id:
                        self.node.openstack_server_id = e.resource_id
                        if self._handleServerFault():
                            self.log.error("Launch attempt failed: %s", str(e))
                            raise exceptions.QuotaException("Quota exceeded")
                        raise
            except Exception as e:
                if is_message_about_quota(str(e)):
                    self.log.error("Launch attempt failed: %s", str(e))
                    raise exceptions.QuotaException("Quota exceeded")
                raise

        if self.state == self.SERVER_CREATING:
            self.server = self.endpoint._refreshServer(self.server)

            if self.server['status'] == 'ACTIVE':
                if (self.label.auto_floating_ip and
                    self.endpoint._needsFloatingIp(self.server)):
                    self.floating_ip = self.endpoint._createFloatingIp(
                        self.server)
                    self.node.openstack_floating_ip_id = self.floating_ip['id']
                    self.state = self.FLOATING_IP_CREATING
                else:
                    self._updateNode()
                    self.state = self.COMPLETE
            elif self.server['status'] == 'ERROR':
                if ('fault' in self.server and self.server['fault'] is not None
                    and 'message' in self.server['fault']):
                    self.log.error(
                        "Error in creating the server."
                        " Compute service reports fault: {reason}".format(
                            reason=self.server['fault']['message']))
                    error_message = self.server['fault']['message']
                    if is_message_about_quota(error_message):
                        raise exceptions.QuotaException("Quota exceeded")
                raise exceptions.LaunchStatusException("Server in error state")
            else:
                return

        if self.state == self.FLOATING_IP_CREATING:
            self.floating_ip = self.endpoint._refreshFloatingIp(
                self.floating_ip)
            if self.floating_ip.get('port_id', None):
                if self.floating_ip['status'] == 'ACTIVE':
                    self.state = self.FLOATING_IP_ATTACHING
                else:
                    return
            else:
                self.endpoint._attachIpToServer(self.server, self.floating_ip)
                self.state = self.FLOATING_IP_ATTACHING

        if self.state == self.FLOATING_IP_ATTACHING:
            self.server = self.endpoint._refreshServer(self.server)
            ext_ip = openstack.cloud.meta.get_server_ip(
                self.server, ext_tag='floating', public=True)
            if ext_ip == self.floating_ip['floating_ip_address']:
                self._updateNode()
                self.state = self.COMPLETE
            else:
                return

        if self.state == self.COMPLETE:
            self.complete = True

    def _updateNode(self):
        instance = self.endpoint._getInstance(self.server, self.node.quota)
        self.node.updateFromInstance(instance)


class OpenstackImageUploadJob(BaseImageUploadJob):
    def __init__(self, endpoint, provider_image, image_name,
                 image_format, metadata, md5, sha256):
        super().__init__()
        self.endpoint = endpoint
        self.provider_image = provider_image
        self.image_name = image_name
        self.image_format = image_format
        self.metadata = metadata
        self.md5 = md5
        self.sha256 = sha256

    def run(self, filename):
        self.endpoint.log.debug(f"Uploading image {self.image_name}")

        image_id = self.endpoint._uploadImage(
            filename,
            self.provider_image, self.image_name,
            self.image_format, self.metadata,
            self.md5, self.sha256)
        return image_id


class OpenstackProviderEndpoint(BaseProviderEndpoint):
    """An OPENSTACK Endpoint corresponds to a single OPENSTACK region,
    and can include multiple availability zones.
    """

    IMAGE_UPLOAD_SLEEP = 30

    def __init__(self, zk_client, driver, connection, region, system_id):
        name = f'{connection.connection_name}-{region}'
        super().__init__(zk_client, driver, connection, name, system_id)
        self.log = logging.getLogger(f"zuul.openstack.{self.name}")
        self.region = region

        # Wrap these instance methods with a per-instance LRU cache so
        # that we don't leak memory over time when the endpoint is
        # occasionally replaced.
        self._findImage = functools.lru_cache(maxsize=None)(
            self._findImage)
        self._listFlavors = functools.lru_cache(maxsize=None)(
            self._listFlavors)
        self._findNetwork = functools.lru_cache(maxsize=None)(
            self._findNetwork)
        self._listAZs = functools.lru_cache(maxsize=None)(
            self._listAZs)

        self.rate_limiter = RateLimiter(self.name, connection.rate)

        self._last_image_check_failure = time.time()
        self._last_port_cleanup = None

    def startEndpoint(self):
        self.log.debug("Starting OpenStack endpoint")
        self._client = self._getClient()
        self._running = True
        # The default http connection pool size is 10; match it for
        # efficiency.
        workers = 10
        self.log.info("Create executor with max workers=%s", workers)
        self.api_executor = ThreadPoolExecutor(
            thread_name_prefix=f'openstack-api-{self.name}',
            max_workers=workers)

        # Use a lazy TTL cache for these.  This uses the TPE to
        # asynchronously update the cached values, meanwhile returning
        # the previous cached data if available.  This means every
        # call after the first one is instantaneous.
        self._listServers = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(
                self._listServers)
        self._listVolumes = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(
                self._listVolumes)
        self._listFloatingIps = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(
                self._listFloatingIps)

    def stopEndpoint(self):
        self.log.debug("Stopping OpenStack endpoint")
        self.api_executor.shutdown()
        self._running = False

    def listResources(self, providers):
        for server in self._listServers():
            if server['status'].lower() == 'deleted':
                continue
            yield OpenstackResource(server.get('metadata', {}),
                                    OpenstackResource.TYPE_INSTANCE,
                                    server['id'])
        # Floating IP and port leakage can't be handled by the
        # automatic resource cleanup in cleanupLeakedResources because
        # openstack doesn't store metadata on those objects, so we
        # call internal cleanup methods here.
        intervals = [p.port_cleanup_interval for p in providers
                     if p.port_cleanup_interval]
        interval = min(intervals or [0])
        if interval:
            self._cleanupLeakedPorts(interval)
        if any([p.floating_ip_cleanup for p in providers]):
            self._cleanupFloatingIps()

    def deleteResource(self, resource):
        self.log.info(f"Deleting leaked {resource.type}: {resource.id}")
        if resource.type == OpenstackResource.TYPE_INSTANCE:
            self._deleteServer(resource.id)

    def listInstances(self):
        volumes = {}
        for volume in self._listVolumes():
            volumes[volume.id] = volume
        for server in self._listServers():
            if server['status'].lower() == 'deleted':
                continue
            flavor = self._getFlavorFromServer(server)
            server_volumes = []
            for vattach in server.get(
                    'os-extended-volumes:volumes_attached', []):
                volume = volumes.get(vattach['id'])
                if volume:
                    server_volumes.append(volume)
            quota = quota_from_flavor(flavor, volumes=server_volumes)
            yield OpenstackInstance(self.connection.cloud_name,
                                    self.getRegionName(), server, quota)

    def getQuotaLimits(self):
        with Timer(self.log, 'API call get_compute_limits'):
            compute = self._client.get_compute_limits()
        try:
            with Timer(self.log, 'API call get_volume_limits'):
                volume = self._client.get_volume_limits()
        except EndpointNotFound:
            volume = None
        return quota_from_limits(compute, volume)

    def refreshQuotaLimits(self, update):
        if self.quota_cache.hasLimits() and not update:
            return False
        limits = self.getQuotaLimits()
        self.quota_cache.setLimits(limits)
        return True

    def getQuotaForLabel(self, label, flavor):
        # This should not rely on the client connection, only the
        # quota cache.
        key = f'flavor-{flavor.flavor_name}'
        return self.quota_cache.getResource(key)

    def refreshQuotaForLabel(self, label, flavor, update):
        key = f'flavor-{flavor.flavor_name}'
        if update or not self.quota_cache.hasResource(key):
            os_flavor = self._findFlavorByName(flavor.flavor_name)
            quota = quota_from_flavor(os_flavor, label=label)
            self.quota_cache.setResource(key, quota)

    def getAZs(self):
        # TODO: This is currently unused; it's unclear if we will need
        # to incorporate this into the node reuse process in the
        # launcher.
        azs = self._listAZs()
        if not azs:
            # If there are no zones, return a list containing None so that
            # random.choice can pick None and pass that to Nova. If this
            # feels dirty, please direct your ire to policy.json and the
            # ability to turn off random portions of the OpenStack API.
            return [None]
        return azs

    def labelReady(self, label):
        if not label.cloud_image:
            return False

        # If an image ID was supplied, we'll assume it is ready since
        # we don't currently have a way of validating that (except during
        # server creation).
        if label.cloud_image.image_id:
            return True

        image = self._findImage(label.cloud_image.external_name)
        if not image:
            self.log.warning(
                "Provider %s is configured to use %s as the"
                " cloud-image for label %s and that"
                " cloud-image could not be found in the"
                " cloud." % (self.provider.name,
                             label.cloud_image.external_name,
                             label.name))
            # If the user insists there should be an image but it
            # isn't in our cache, invalidate the cache periodically so
            # that we can see new cloud image uploads.
            if (time.time() - self._last_image_check_failure >
                self.IMAGE_CHECK_TIMEOUT):
                self._findImage.cache_clear()
                self._last_image_check_failure = time.time()
            return False
        return True

    def getImageUploadJob(self, provider_image, image_name,
                          image_format, metadata, md5, sha256):
        return OpenstackImageUploadJob(
            self,
            provider_image, image_name,
            image_format, metadata, md5, sha256)

    def _uploadImage(self, filename, provider_image, image_name,
                     image_format, metadata, md5, sha256):
        # configure glance and upload image.  Note the meta flags
        # are provided as custom glance properties
        # NOTE: we have wait=True set here. This is not how we normally
        # do things in nodepool, preferring to poll ourselves thankyouverymuch.
        # However - two things to note:
        #  - PUT has no aysnc mechanism, so we have to handle it anyway
        #  - v2 w/task waiting is very strange and complex - but we have to
        #              block for our v1 clouds anyway, so we might as well
        #              have the interface be the same and treat faking-out
        #              a openstacksdk-level fake-async interface later
        timeout = provider_image.import_timeout
        if not metadata:
            metadata = {}
        if image_format:
            metadata['disk_format'] = image_format
        with Timer(self.log, 'API call create_image'):
            image = self._client.create_image(
                name=image_name,
                filename=filename,
                is_public=False,
                wait=True,
                md5=md5,
                sha256=sha256,
                timeout=timeout,
                **metadata)
        return image.id

    def deleteImage(self, external_id):
        self.log.debug(f"Deleting image {external_id}")
        with Timer(self.log, 'API call delete_image'):
            return self._client.delete_image(external_id)

    # Local implementation

    def _getInstance(self, server, quota):
        return OpenstackInstance(
            self.connection.cloud_name,
            self.getRegionName(),
            server, quota)

    def _getRegionConfig(self):
        config = openstack.config.OpenStackConfig(
            config_files=self.connection.config_files,
            load_envvars=False,
            app_name='zuul',
        )
        return config.get_one(cloud=self.connection.cloud_name,
                              region_name=self.region)

    def _getClient(self):
        region = self._getRegionConfig()
        # Note: the connection object holds resources internally that
        # prevent GC.  Currently our endpoints are long-lived, and we
        # don't perform any cleanup for them.  If we do in the future,
        # we will need to call Connection.close() to release
        # resources.
        return openstack.connection.Connection(
            config=region,
            use_direct_get=False,
            rate_limit=self.connection.rate,
        )

    def getImageFormat(self):
        region = self._getRegionConfig()
        return region.config['image_format']

    def getRegionName(self):
        # With OpenStackSDK, users can omit the region and the SDK may
        # supply a default region.  This helper method will return the
        # actual region in use regardless of what the user supplied.
        return self._client.config.config['region_name']

    def _submitApi(self, api, *args, **kw):
        return self.api_executor.submit(
            api, *args, **kw)

    def _completeApi(self, future):
        if not future.done():
            return None
        return future.result()

    def _createServer(self, name, image, flavor,
                      az=None, key_name=None, config_drive=True,
                      networks=None, security_groups=None,
                      boot_from_volume=False, volume_size=50,
                      instance_properties=None, userdata=None):
        if not networks:
            networks = []
        if not isinstance(image, dict):
            # if it's a dict, we already have the cloud id. If it's not,
            # we don't know if it's name or ID so need to look it up
            image = self._findImage(image)
        create_args = dict(name=name,
                           image=image,
                           flavor=flavor,
                           config_drive=config_drive)
        if boot_from_volume:
            create_args['boot_from_volume'] = boot_from_volume
            create_args['volume_size'] = volume_size
            # NOTE(pabelanger): Always cleanup volumes when we delete a server.
            create_args['terminate_volume'] = True
        if key_name:
            create_args['key_name'] = key_name
        if az:
            create_args['availability_zone'] = az
        if security_groups:
            create_args['security_groups'] = security_groups
        if userdata:
            create_args['userdata'] = userdata
        nics = []
        for network in networks:
            net_id = self._findNetwork(network)['id']
            nics.append({'net-id': net_id})
        if nics:
            create_args['nics'] = nics
        if instance_properties:
            create_args['meta'] = instance_properties

        try:
            with Timer(self.log, 'API call create_server'):
                return self._client.create_server(wait=False, **create_args)
        except openstack.exceptions.BadRequestException:
            # We've gotten a 400 error from nova - which means the request
            # was malformed. The most likely cause of that, unless something
            # became functionally and systemically broken, is stale az, image
            # or flavor cache. Log a message, invalidate the caches so that
            # next time we get new caches.
            self.log.info(
                "Clearing az, flavor and image caches due to 400 error "
                "from nova")
            self._findImage.cache_clear()
            self._listFlavors.cache_clear()
            self._findNetwork.cache_clear()
            self._listAZs.cache_clear()
            raise

    # This method is wrapped with an LRU cache in the constructor.
    def _listAZs(self):
        with Timer(self.log, 'API call list_availability_zone_names'):
            return self._client.list_availability_zone_names()

    # This method is wrapped with an LRU cache in the constructor.
    def _findImage(self, name):
        with Timer(self.log, 'API call get_image'):
            return self._client.get_image(name, filters={'status': 'active'})

    # This method is wrapped with an LRU cache in the constructor.
    def _listFlavors(self):
        with Timer(self.log, 'API call list_flavors'):
            flavors = self._client.list_flavors(get_extra=False)
        flavors.sort(key=operator.itemgetter('ram', 'name'))
        return flavors

    # This method is only used by the nodepool alien-image-list
    # command and only works with the openstack driver.
    def _listImages(self):
        with Timer(self.log, 'API call list_images'):
            return self._client.list_images()

    def _findFlavorByName(self, flavor_name):
        for f in self._listFlavors():
            if flavor_name in (f['name'], f['id']):
                return f
        raise Exception("Unable to find flavor: %s" % flavor_name)

    def _findFlavorByRam(self, min_ram, flavor_name):
        for f in self._listFlavors():
            if (f['ram'] >= min_ram
                    and (not flavor_name or flavor_name in f['name'])):
                return f
        raise Exception("Unable to find flavor with min ram: %s" % min_ram)

    def _findFlavorById(self, flavor_id):
        for f in self._listFlavors():
            if f['id'] == flavor_id:
                return f
        raise Exception("Unable to find flavor with id: %s" % flavor_id)

    def _findFlavor(self, flavor_name, min_ram=None):
        if min_ram:
            return self._findFlavorByRam(min_ram, flavor_name)
        else:
            return self._findFlavorByName(flavor_name)

    # This method is wrapped with an LRU cache in the constructor.
    def _findNetwork(self, name):
        with Timer(self.log, 'API call get_network'):
            network = self._client.get_network(name)
            self.log.debug('Found network %s: %s', name, network)
        if not network:
            raise Exception("Unable to find network %s in endpoint %s" % (
                name, self.name))
        return network

    # This method is based on code from OpenStackSDK, licensed
    # under ASL2.
    def _simpleServerList(self):
        session = self._client.compute
        limit = None
        query_params = {}
        uri = '/servers/detail'
        ret = []
        while uri:
            response = session.get(
                uri,
                headers={"Accept": "application/json"},
                params=query_params,
            )
            data = response.json()

            last_marker = query_params.pop('marker', None)
            query_params.pop('limit', None)

            resources = data['servers']
            if not isinstance(resources, list):
                resources = [resources]

            ret += [ZuulOpenstackServer(x) for x in resources]

            if resources:
                marker = resources[-1]['id']
                uri, next_params = self._getNextLink(
                    uri, response, data, marker, limit
                )
                try:
                    if next_params['marker'] == last_marker:
                        raise Exception(
                            'Endless pagination loop detected, aborting'
                        )
                except KeyError:
                    pass
                query_params.update(next_params)
            else:
                break
        return ret

    # This method is based on code from OpenStackSDK, licensed
    # under ASL2.
    def _getNextLink(self, uri, response, data, marker, limit):
        pagination_key = 'servers_links'
        next_link = None
        params = {}

        if isinstance(data, dict):
            links = data.get(pagination_key, {})

            for item in links:
                if item.get('rel') == 'next' and 'href' in item:
                    next_link = item['href']
                    break

            if next_link and next_link.startswith('/v'):
                next_link = next_link[next_link.find('/', 1):]

        if not next_link and 'next' in response.links:
            # RFC5988 specifies Link headers and requests parses them if they
            # are there. We prefer link dicts in resource body, but if those
            # aren't there and Link headers are, use them.
            next_link = response.links['next']['uri']

        # Parse params from Link (next page URL) into params.
        # This prevents duplication of query parameters that with large
        # number of pages result in HTTP 414 error eventually.
        if next_link:
            parts = urllib.parse.urlparse(next_link)
            query_params = urllib.parse.parse_qs(parts.query)
            params.update(query_params)
            next_link = urllib.parse.urljoin(next_link, parts.path)

        # If we still have no link, and limit was given and is non-zero,
        # and the number of records yielded equals the limit, then the user
        # is playing pagination ball so we should go ahead and try once more.
        if not next_link and limit:
            next_link = uri
            params['marker'] = marker
            params['limit'] = limit

        return next_link, params

    def _listServers(self):
        with Timer(self.log, 'API call detailed server list'):
            return self._simpleServerList()

    def _listVolumes(self):
        try:
            with Timer(self.log, 'API call list_volumes'):
                return self._client.list_volumes()
        except EndpointNotFound:
            return []

    def _listFloatingIps(self):
        with Timer(self.log, 'API call list_floating_ips'):
            return self._client.list_floating_ips()

    def _refreshServer(self, obj):
        ret = self._getServer(obj['id'])
        if ret:
            return ret
        return obj

    def _expandServer(self, server):
        return openstack.cloud.meta.add_server_interfaces(
            self._client, server)

    def _getServer(self, external_id):
        for server in self._listServers():
            if server['id'] == external_id:
                if server['status'] in ['ACTIVE', 'ERROR']:
                    return self._expandServer(server)
                return server
        return None

    def _getServerByIdNow(self, server_id):
        # A synchronous get server by id.  Only to be used in error
        # handling where we can't wait for the list to update.
        with Timer(self.log, 'API call get_server_by_id'):
            return self._client.get_server_by_id(server_id)

    def _refreshServerDelete(self, obj):
        if obj is None:
            return obj
        for server in self._listServers():
            if server['id'] == obj['id']:
                if server['status'].lower() == 'deleted':
                    return None
                return server
        return None

    def _refreshFloatingIp(self, obj):
        for fip in self._listFloatingIps():
            if fip['id'] == obj['id']:
                return fip
        return obj

    def _refreshFloatingIpDelete(self, obj):
        if obj is None:
            return obj
        for fip in self._listFloatingIps():
            if fip['id'] == obj['id']:
                if fip.status == 'DOWN':
                    return None
                return fip
        return None

    def _needsFloatingIp(self, server):
        with Timer(self.log, 'API call _needs_floating_ip'):
            return self._client._needs_floating_ip(
                server=server, nat_destination=None)

    def _createFloatingIp(self, server):
        with Timer(self.log, 'API call create_floating_ip'):
            return self._client.create_floating_ip(server=server, wait=True)

    def _attachIpToServer(self, server, fip):
        # skip_attach is ignored for nova, which is the only time we
        # should actually call this method.
        with Timer(self.log, 'API call _attach_ip_to_server'):
            return self._client._attach_ip_to_server(
                server=server, floating_ip=fip,
                skip_attach=True)

    def _hasFloatingIps(self):
        # Not a network call
        return self._client._has_floating_ips()

    def _getFloatingIps(self, server):
        fips = openstack.cloud.meta.find_nova_interfaces(
            server['addresses'], ext_tag='floating')
        ret = []
        for fip in fips:
            with Timer(self.log, 'API call get_floating_ip'):
                ret.append(self._client.get_floating_ip(
                    id=None, filters={'floating_ip_address': fip['addr']}))
        return ret

    def _deleteFloatingIp(self, fip):
        with Timer(self.log, 'API call delete_floating_ip'):
            self._client.delete_floating_ip(fip['id'], retry=0)

    def _deleteServer(self, external_id):
        with Timer(self.log, 'API call delete_server'):
            self._client.delete_server(external_id)
        return True

    def _getFlavorFromServer(self, server):
        # In earlier versions of nova or the sdk, flavor has just an id.
        # In later versions it returns the information we're looking for.
        # If we get the information we want, we do not need to try to
        # lookup the flavor in our list.
        if hasattr(server['flavor'], 'vcpus'):
            return server['flavor']
        else:
            return self._findFlavorById(server['flavor']['id'])

    # The port cleanup logic.  We don't get tags or metadata, so we
    # have to figure this out on our own.

    # This method is not cached
    def _listPorts(self, status=None):
        '''
        List known ports.

        :param str status: A valid port status. E.g., 'ACTIVE' or 'DOWN'.
        '''
        if status:
            ports = self._client.list_ports(filters={'status': status})
        else:
            ports = self._client.list_ports()
        return ports

    def _filterComputePorts(self, ports):
        '''
        Return a list of compute ports (or no device owner).

        We are not interested in ports for routers or DHCP.
        '''
        ret = []
        for p in ports:
            if (p.device_owner is None or p.device_owner == '' or
                    p.device_owner.startswith("compute:")):
                ret.append(p)
        return ret

    def _cleanupLeakedPorts(self, interval):
        if not self._last_port_cleanup:
            self._last_port_cleanup = time.monotonic()
            ports = self._listPorts(status='DOWN')
            ports = self._filterComputePorts(ports)
            self._down_ports = set([p.id for p in ports])
            return

        # Return if not enough time has passed between cleanup
        last_check_in_secs = int(time.monotonic() - self._last_port_cleanup)
        if last_check_in_secs <= interval:
            return

        ports = self._listPorts(status='DOWN')
        ports = self._filterComputePorts(ports)
        current_set = set([p.id for p in ports])
        remove_set = current_set & self._down_ports

        removed_count = 0
        for port_id in remove_set:
            try:
                self._deletePort(port_id)
            except Exception:
                self.log.exception("Exception deleting port %s in %s:",
                                   port_id, self.name)
            else:
                removed_count += 1
                self.log.debug("Removed DOWN port %s in %s",
                               port_id, self.name)

        # if self._statsd and removed_count:
        #     key = 'nodepool.provider.%s.leaked.ports' % (self.name)
        #     self._statsd.incr(key, removed_count)

        self._last_port_cleanup = time.monotonic()

        # Rely on OpenStack to tell us the down ports rather than doing our
        # own set adjustment.
        ports = self._listPorts(status='DOWN')
        ports = self._filterComputePorts(ports)
        self._down_ports = set([p.id for p in ports])

    def _deletePort(self, port_id):
        self._client.delete_port(port_id)

    # This method is vendored from openstacksdk (Apache 2 licensed) then
    # modified to use our client object rather than self in order to add
    # additional debugging to the floating ip deletion process. This code
    # originates from:
    # https://opendev.org/openstack/openstacksdk/src/tag/4.8.0/openstack/cloud/_network_common.py#L971-L1001
    def _delete_unattached_floating_ips(self, retry=1):
        """Safely delete unattached floating ips.

        If the cloud can safely purge any unattached floating ips without
        race conditions, do so.

        Safely here means a specific thing. It means that you are not running
        this while another process that might do a two step create/attach
        is running. You can safely run this  method while another process
        is creating servers and attaching floating IPs to them if either that
        process is using add_auto_ip from shade, or is creating the floating
        IPs by passing in a server to the create_floating_ip call.

        :param retry: number of times to retry. Optional, defaults to 1,
            which is in addition to the initial delete call.
            A value of 0 will also cause no checking of results to occur.

        :returns: Number of Floating IPs deleted, False if none
        :raises: :class:`~openstack.exceptions.SDKException` on operation
            error.
        """
        processed = []
        if self._client._use_neutron_floating():
            for ip in self._client.list_floating_ips():
                if not bool(ip.port_id):
                    self.log.debug("Deleting unattached floating ip %s %s",
                                   ip['id'], ip['floating_ip_address'])
                    processed.append(
                        self._client.delete_floating_ip(
                            floating_ip_id=ip['id'], retry=retry
                        )
                    )
        return len(processed) if all(processed) else False

    def _cleanupFloatingIps(self):
        did_clean = self._delete_unattached_floating_ips()
        if did_clean:
            # some openstacksdk's return True if any port was
            # cleaned, rather than the count.  Just set it to 1 to
            # indicate something happened.
            if type(did_clean) is bool:
                did_clean = 1
            # if self._statsd:
            #     key = ('nodepool.provider.%s.leaked.floatingips'
            #            % self.name)
            #     self._statsd.incr(key, did_clean)

    def getConsoleLog(self, label, external_id):
        if not label.console_log:
            return None
        try:
            return self._client.get_server_console(external_id)
        except openstack.exceptions.OpenStackCloudException:
            return None
