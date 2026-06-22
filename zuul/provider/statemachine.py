# Copyright 2021-2024 Acme Gating, LLC
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

import time


class StateMachine:
    START = 'start'
    COMPLETE = 'complete'

    def __init__(self, initial_state):
        self.state = initial_state.get("state", self.START)
        self.complete = False
        self.start_time = initial_state.get("start_time", time.time())

    def advance(self):
        pass

    def toDict(self):
        return dict(
            state=self.state,
            start_time=self.start_time,
        )

    def fail(self):
        self.state = self.COMPLETE
        self.complete = True


class Instance:
    """Represents a cloud instance

    This class is used by the State Machine Driver classes to
    represent a standardized version of a remote cloud instance.
    Implement this class in your driver, override the :py:meth:`load`
    method, and supply as many of the fields as possible.

    The following attributes are required:

    * ready: bool (whether the instance is ready)
    * deleted: bool (whether the instance is in a deleted state)
    * interface_ip: str
    * metadata: dict

    The following are optional:

    * public_ipv4: str
    * public_ipv6: str
    * private_ipv4: str
    * cloud: str
    * az: str
    * region: str
    * host_id: str
    * driver_data: any
    * slot: int

    And the following are even more optional (as they are usually
    already set from the image configuration):

    * username: str
    * python_path: str
    * shell_type: str
    * connection_port: str
    * connection_type: str
    * host_keys: [str]

    This is extremely optional, in fact, it's difficult to imagine
    that it's useful for anything other than the metastatic driver:

    * node_attributes: dict

    """
    def __init__(self):
        self.ready = False
        self.deleted = False
        self.public_ipv4 = None
        self.public_ipv6 = None
        self.private_ipv4 = None
        self.private_ipv6 = None
        self.interface_ip = None
        self.cloud = None
        self.az = None
        self.region = None
        self.host_id = None
        self.metadata = {}
        self.driver_data = None

    @property
    def external_id(self):
        """Return a string representation of external ID(s) for this instance,
        for use in logging"""
        raise NotImplementedError()

    def __repr__(self):
        state = []
        if self.ready:
            state.append('ready')
        if self.deleted:
            state.append('deleted')
        state = ' '.join(state)
        return '<{klass} {external_id} {state}>'.format(
            klass=self.__class__.__name__,
            external_id=self.external_id,
            state=state)

    def getQuotaInformation(self):
        """Return quota information about this instance.

        :returns: A :py:class:`QuotaInformation` object.
        """
        raise NotImplementedError()


class Resource:
    """Represents a cloud resource

    This could be an instance, a disk, a floating IP, or anything
    else.  It is used by the driver to detect leaked resources so the
    adapter can clean them up.

    The `type` attribute should be an alphanumeric string suitable for
    inclusion in a statsd metric name.

    The `metadata` attribute is a dictionary of key/value pairs
    initially supplied by the driver to the adapter when an instance
    or image was created.  This is used by the driver to detect leaked
    resources.  The adapter may add any other information to this
    instance for its own bookeeping (resource type, id, etc).

    :param str type: The type of resource.
    :param dict metadata: A dictionary of metadata for the resource.

    """

    def __init__(self, metadata, type):
        self.type = type
        self.metadata = metadata

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.type} {self.metadata}>'

    @property
    def unique_id(self):
        """Return a unique id string for this resource"""
        raise NotImplementedError()
