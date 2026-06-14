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

import concurrent.futures
import json
import logging
import time

import requests


class AzureAuth(requests.auth.AuthBase):
    AUTH_URL = "https://login.microsoftonline.com/{tenantId}/oauth2/token"
    # https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow#first-case-access-token-request-with-a-shared-secret
    # https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow#third-case-access-token-request-with-a-federated-credential

    def __init__(self, subscription_id, tenant_id, client_id,
                 client_secret, federated_token_file):
        self.log = logging.getLogger("azul.auth")
        self.subscription_id = subscription_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.federated_token_file = federated_token_file
        self.token = None
        self.expiration = 0

    def refresh(self):
        if self.expiration - time.time() >= 60:
            return
        if self.federated_token_file:
            response = self.refresh_federated()
        else:
            response = self.refresh_secret()
        ret = response.json()
        if 'access_token' in ret:
            self.token = ret['access_token']
            self.expiration = float(ret['expires_on'])
        elif ('error_codes' in ret and 'error_description' in ret):
            raise AzureError(response.status_code,
                             ret['error_codes'][0],
                             ret['error_description'])
        else:
            raise Exception("Unknown error when authenticating to Azure")

    def load_from_token_file(self):
        with open(self.auth_location, encoding="utf-8") as f:
            credential = json.load(f)
        self.subscription_id = credential['subscriptionId']
        return credential

    def refresh_secret(self):
        self.log.debug('Refreshing secret authentication token')
        url = self.AUTH_URL.format(tenantId=self.tenant_id)
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'resource': 'https://management.azure.com/',
        }
        return requests.post(url, data)

    def refresh_federated(self):
        self.log.debug('Refreshing federated authentication token')
        url = self.AUTH_URL.format(tenantId=self.tenant_id)
        with open(self.federated_token_file, encoding="utf-8") as f:
            token = f.read()
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'resource': 'https://management.azure.com/',
            'client_assertion_type':
            'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
            'client_assertion': token,
        }
        return requests.post(url, data)

    def __call__(self, r):
        self.refresh()
        r.headers["authorization"] = "Bearer " + self.token
        return r


class AzureError(Exception):
    def __init__(self, status_code, error_code, message):
        super().__init__(message)
        self.error_code = error_code
        self.status_code = status_code


class AzureNotFoundError(AzureError):
    pass


class AzureCRUD:
    base_subscription_url = '/subscriptions/{subscriptionId}/'
    base_url = ''

    def __init__(self, cloud, **kw):
        self.cloud = cloud
        self.args = kw.copy()
        self.args.update(subscriptionId=self.cloud.subscription_id)

    def id(self, endpoint=None, **kw):
        if endpoint is None:
            endpoint = ''
        else:
            endpoint = '/' + endpoint
        url = (self.base_subscription_url + self.base_url + endpoint)
        args = self.args.copy()
        args.update(kw)
        return url.format(**args)

    def url(self, endpoint=None, **kw):
        path = self.id(endpoint, **kw)
        query = path + '?api-version={apiVersion}'.format(**self.args)
        return 'https://management.azure.com' + query

    def id_url(self, url, **kw):
        base_url = 'https://management.azure.com'
        url = base_url + url + '?api-version={apiVersion}'
        args = self.args.copy()
        args.update(kw)
        return url.format(**args)

    def get_by_id(self, resource_id):
        url = self.id_url(resource_id)
        return self.cloud.get(url)

    def _list(self, **kw):
        url = self.url(**kw)
        return self.cloud.paginate(self.cloud.get(url))

    def list(self):
        return self._list()

    def _get(self, **kw):
        url = self.url(**kw)
        return self.cloud.get(url)

    def _create(self, params, **kw):
        url = self.url(**kw)
        return self.cloud.put(url, params)

    def _delete(self, **kw):
        url = self.url(**kw)
        return self.cloud.delete(url)

    def _post(self, endpoint, params, **kw):
        url = self.url(endpoint=endpoint, **kw)
        return self.cloud.post(url, params)

    def delete_by_id(self, id):
        url = self.id_url(id)
        return self.cloud.delete(url)


class AzureResourceGroupsCRUD(AzureCRUD):
    base_url = 'resourcegroups/{resourceGroupName}'

    def list(self):
        return self._list(resourceGroupName='')

    def get(self, name):
        return self._get(resourceGroupName=name)

    def create(self, name, params):
        return self._create(params, resourceGroupName=name)

    def delete(self, name):
        return self._delete(resourceGroupName=name)


class AzureGroupedProviderCRUD(AzureCRUD):
    base_url = (
        'resourceGroups/{resourceGroupName}/providers/'
        '{providerId}/{resource}/{resourceName}')

    def list(self, resource_group_name):
        return self._list(resourceGroupName=resource_group_name,
                          resourceName='')

    def get(self, resource_group_name, name):
        return self._get(resourceGroupName=resource_group_name,
                         resourceName=name)

    def create(self, resource_group_name, name, params):
        return self._create(params,
                            resourceGroupName=resource_group_name,
                            resourceName=name)

    def delete(self, resource_group_name, name):
        return self._delete(resourceGroupName=resource_group_name,
                            resourceName=name)

    def post(self, resource_group_name, name, endpoint, params):
        return self._post(endpoint, params,
                          resourceGroupName=resource_group_name,
                          resourceName=name)


class AzureProviderCRUD(AzureCRUD):
    base_url = (
        'providers/{providerId}/{resource}/{resourceName}'
    )

    def list(self):
        return self._list(resourceName='')

    def get(self, name):
        return self._get(resourceName=name)

    def create(self, name, params):
        return self._create(params,
                            resourceName=name)

    def delete(self, name):
        return self._delete(resourceName=name)

    def post(self, name, endpoint, params):
        return self._post(endpoint, params,
                          resourceName=name)


class AzureNetworkCRUD(AzureCRUD):
    base_url = (
        'resourceGroups/{resourceGroupName}/providers/'
        'Microsoft.Network/virtualNetworks/{virtualNetworkName}/'
        '{resource}/{resourceName}')

    def list(self, resource_group_name, virtual_network_name):
        return self._list(resourceGroupName=resource_group_name,
                          virtualNetworkName=virtual_network_name,
                          resourceName='')

    def get(self, resource_group_name, virtual_network_name, name):
        return self._get(resourceGroupName=resource_group_name,
                         virtualNetworkName=virtual_network_name,
                         resourceName=name)

    def create(self, resource_group_name, virtual_network_name, name, params):
        return self._create(params,
                            resourceGroupName=resource_group_name,
                            virtualNetworkName=virtual_network_name,
                            resourceName=name)

    def delete(self, resource_group_name, virtual_network_name, name):
        return self._delete(resourceGroupName=resource_group_name,
                            virtualNetworkName=virtual_network_name,
                            resourceName=name)


class AzureLocationCRUD(AzureCRUD):
    base_url = (
        'providers/{providerId}/locations/{location}/{resource}')

    def list(self, location):
        return self._list(location=location)


class AzureDictResponse(dict):
    def __init__(self, response, *args):
        super().__init__(*args)
        self.response = response
        self.last_retry = time.time()


class AzureListResponse(list):
    def __init__(self, response, *args):
        super().__init__(*args)
        self.response = response
        self.last_retry = time.time()


class AzureCloud:
    TIMEOUT = 60

    def __init__(self, subscription_id=None, tenant_id=None,
                 client_id=None, client_secret=None,
                 federated_token_file=None):
        self.session = requests.Session()
        self.log = logging.getLogger("azul")
        self.auth = AzureAuth(subscription_id, tenant_id, client_id,
                              client_secret, federated_token_file)
        self.subscription_id = subscription_id or self.auth.subscription_id
        self.resource_group_network_interfaces = AzureGroupedProviderCRUD(
            self,
            providerId='Microsoft.Network',
            resource='networkInterfaces',
            apiVersion='2020-11-01')
        self.network_interfaces = AzureProviderCRUD(
            self,
            providerId='Microsoft.Network',
            resource='networkInterfaces',
            apiVersion='2020-11-01')
        self.resource_group_public_ip_addresses = AzureGroupedProviderCRUD(
            self,
            providerId='Microsoft.Network',
            resource='publicIPAddresses',
            apiVersion='2020-11-01')
        self.public_ip_addresses = AzureProviderCRUD(
            self,
            providerId='Microsoft.Network',
            resource='publicIPAddresses',
            apiVersion='2020-11-01')
        self.resource_group_virtual_machines = AzureGroupedProviderCRUD(
            self,
            providerId='Microsoft.Compute',
            resource='virtualMachines',
            apiVersion='2025-11-01')
        self.virtual_machines = AzureProviderCRUD(
            self,
            providerId='Microsoft.Compute',
            resource='virtualMachines',
            apiVersion='2025-11-01')
        self.resource_group_disks = AzureGroupedProviderCRUD(
            self,
            providerId='Microsoft.Compute',
            resource='disks',
            apiVersion='2020-06-30')
        self.disks = AzureProviderCRUD(
            self,
            providerId='Microsoft.Compute',
            resource='disks',
            apiVersion='2020-06-30')
        self.resource_group_images = AzureGroupedProviderCRUD(
            self,
            providerId='Microsoft.Compute',
            resource='images',
            apiVersion='2020-12-01')
        self.images = AzureProviderCRUD(
            self,
            providerId='Microsoft.Compute',
            resource='images',
            apiVersion='2020-12-01')
        self.resource_groups = AzureResourceGroupsCRUD(
            self,
            apiVersion='2020-06-01')
        self.subnets = AzureNetworkCRUD(
            self,
            resource='subnets',
            apiVersion='2020-07-01')
        self.compute_usages = AzureLocationCRUD(
            self,
            providerId='Microsoft.Compute',
            resource='usages',
            apiVersion='2020-12-01')
        self.compute_skus = AzureProviderCRUD(
            self,
            providerId='Microsoft.Compute',
            resource='skus',
            apiVersion='2019-04-01')
        self.resource_group_managed_identities = AzureGroupedProviderCRUD(
            self,
            providerId='Microsoft.ManagedIdentity',
            resource='userAssignedIdentities',
            apiVersion='2023-01-31')
        self.managed_identities = AzureProviderCRUD(
            self,
            providerId='Microsoft.ManagedIdentity',
            resource='userAssignedIdentities',
            apiVersion='2023-01-31')

    def get(self, url, codes=[200]):
        return self.request('GET', url, None, codes)

    def put(self, url, data, codes=[200, 201, 202]):
        return self.request('PUT', url, data, codes)

    def post(self, url, data, codes=[200, 202]):
        return self.request('POST', url, data, codes)

    def delete(self, url, codes=[200, 201, 202, 204]):
        return self.request('DELETE', url, None, codes)

    def request(self, method, url, data, codes):
        self.log.debug('%s: %s %s' % (method, url, data))
        response = self.session.request(
            method, url, json=data,
            auth=self.auth, timeout=self.TIMEOUT,
            headers={'Accept': 'application/json',
                     'Accept-Encoding': 'gzip'})

        self.log.debug("Received headers: %s", response.headers)
        if response.status_code in codes:
            if len(response.text):
                self.log.debug("Received: %s", response.text)
                ret_data = response.json()
                if isinstance(ret_data, list):
                    return AzureListResponse(response, ret_data)
                else:
                    return AzureDictResponse(response, ret_data)
            self.log.debug("Empty response")
            return AzureDictResponse(response, {})
        err = response.json()
        self.log.error(response.text)
        if response.status_code == 404:
            raise AzureNotFoundError(
                response.status_code,
                err['error']['code'],
                err['error']['message'])
        else:
            raise AzureError(response.status_code,
                             err['error']['code'],
                             err['error']['message'])

    def paginate(self, data):
        ret = data['value']
        while 'nextLink' in data:
            data = self.get(data['nextLink'])
            ret += data['value']
        return ret

    def check_async_operation(self, response):
        resp = response.response
        location = resp.headers.get(
            'Azure-AsyncOperation',
            resp.headers.get('Location', None))
        if not location:
            self.log.debug("No async operation found")
            return None
        remain = (response.last_retry +
                  float(resp.headers.get('Retry-After', 2))) - time.time()
        self.log.debug("remain time %s", remain)
        if remain > 0:
            time.sleep(remain)
        response.last_retry = time.time()
        return self.get(location)

    def wait_for_async_operation(self, response, timeout=600):
        start = time.time()
        while True:
            if time.time() - start > timeout:
                raise Exception("Timeout waiting for async operation")
            ret = self.check_async_operation(response)
            if ret is None:
                return
            if ret['status'] == 'InProgress':
                continue
            if ret['status'] == 'Succeeded':
                return ret
            raise Exception("Unhandled async operation result: %s",
                            ret['status'])

    def upload_sas_chunk(self, url, start, end, data):
        if 'comp=page' not in url:
            url += '&comp=page'
        headers = {
            'x-ms-blob-type': 'PageBlob',
            'x-ms-page-write': 'Update',
            'Content-Length': str(len(data)),
            'Range': f'bytes={start}-{end}',
        }
        requests.put(url, headers=headers, data=data).raise_for_status()

    def _upload_chunk(self, url, start, end, data):
        attempts = 10
        for x in range(attempts):
            try:
                self.upload_sas_chunk(url, start, end, data)
                break
            except Exception:
                if x == attempts - 1:
                    raise
                else:
                    time.sleep(2 * x)

    def upload_page_blob_to_sas_url(self, url, file_object,
                                    pagesize=(4 * 1024 * 1024),
                                    concurrency=10):
        start = 0
        futures = set()
        if 'comp=page' not in url:
            url += '&comp=page'
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=concurrency) as executor:
            while True:
                chunk = file_object.read(pagesize)
                if not chunk:
                    break
                end = start + len(chunk) - 1
                future = executor.submit(self._upload_chunk, url,
                                         start, end, chunk)
                start += len(chunk)
                futures.add(future)
                # Keep the pool of work supplied with data but without
                # reading the entire file into memory.
                if len(futures) >= (concurrency * 2):
                    (done, futures) = concurrent.futures.wait(
                        futures,
                        return_when=concurrent.futures.FIRST_COMPLETED)
            # We're done reading the file, wait for all uploads to finish
            (done, futures) = concurrent.futures.wait(futures)
