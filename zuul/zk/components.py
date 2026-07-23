# Copyright 2020 BMW Group
# Copyright 2021-2026 Acme Gating, LLC
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
import sys
import json
import logging
import threading
from collections import defaultdict

from kazoo.exceptions import NoNodeError
from kazoo.protocol.states import EventType

from zuul.zk import ZooKeeperBase
from zuul.model_api import MODEL_API


COMPONENTS_ROOT = "/zuul/components"


class GlobalRegistry:
    def __init__(self):
        self.registry = None

    def create(self, zk_client):
        if not self.registry:
            self.registry = ComponentRegistry(zk_client)
        return self.registry

    def clearRegistry(self):
        self.registry = None

    @property
    def model_api(self):
        return self.registry.model_api


COMPONENT_REGISTRY = GlobalRegistry()


class BaseComponent(ZooKeeperBase):
    """
    Read/write component object.

    This object holds an offline cache of all the component's attributes. In
    case of an failed update to ZooKeeper the local cache will still hold the
    fresh values. Updating any attribute uploads all attributes to ZooKeeper.

    This enables this object to be used as local key/value store even if the
    ZooKeeper connection got lost.

    :arg client ZooKeeperClient: The Zuul ZooKeeperClient object to use, or
         None if this should be a read-only component.
    :arg hostname str: The component's hostname (multiple components with
         the same hostname may be registered; the registry will create unique
         nodes for each).
    """

    # Component states
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"

    log = logging.getLogger("zuul.Component")
    kind = "base"

    def __init__(self, client, hostname, version=None):
        # Ensure that the content is available before setting any other
        # attribute, because our __setattr__ implementation is relying on it.
        self.__dict__["content"] = {
            "hostname": hostname,
            "state": self.STOPPED,
            "kind": self.kind,
            "version": version,
            "model_api": 0,
        }
        super().__init__(client)

        self.path = None
        self.register_lock = threading.Lock()

    def __getattr__(self, name):
        try:
            return self.content[name]
        except KeyError:
            raise AttributeError

    def __setattr__(self, name, value):
        # If the specified attribute is not part of our content dictionary,
        # fall back to the default __settattr__ behaviour.
        if name not in self.content.keys():
            return super().__setattr__(name, value)

        # Set the value in the local content dict
        self.content[name] = value

        with self.register_lock:
            if not self.path:
                self.log.error(
                    "Path is not set on this component, did you forget "
                    "to call register()?"
                )
                return

            # Update the ZooKeeper node
            content = json.dumps(self.content, sort_keys=True).encode("utf-8")
            try:
                self.kazoo_client.set(self.path, content)
            except NoNodeError:
                self.log.error("Could not update %s in ZooKeeper", self)

    def register(self, model_api=MODEL_API):
        self.content['model_api'] = model_api
        with self.register_lock:
            path = "/".join([COMPONENTS_ROOT, self.kind, self.hostname])
            self.log.info("Registering component in ZooKeeper %s", path)
            self.path = self.kazoo_client.create(
                path,
                json.dumps(self.content, sort_keys=True).encode("utf-8"),
                makepath=True,
                ephemeral=True,
                sequence=True,
            )

    def _onReconnect(self):
        self.register()

    def updateFromDict(self, data):
        self.content.update(data)

    @classmethod
    def fromDict(cls, client, hostname, data):
        component = cls(client, hostname)
        component.updateFromDict(data)
        return component

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.content}>"


class SchedulerComponent(BaseComponent):
    kind = "scheduler"

    def __init__(self, *args, **kwargs):
        keystore_passwords = kwargs.pop('keystore_passwords', [])
        super().__init__(*args, **kwargs)
        self.initial_state = {
            "keystore_passwords": keystore_passwords
        }
        self.content.update(self.initial_state)


class ExecutorComponent(BaseComponent):
    kind = "executor"

    def __init__(self, *args, **kwargs):
        keystore_passwords = kwargs.pop('keystore_passwords', [])
        super().__init__(*args, **kwargs)
        self.initial_state = {
            "accepting_work": False,
            "zone": None,
            "allow_unzoned": False,
            "process_merge_jobs": False,
            "keystore_passwords": keystore_passwords,
        }
        self.content.update(self.initial_state)


class MergerComponent(BaseComponent):
    kind = "merger"


class FingerGatewayComponent(BaseComponent):
    kind = "fingergw"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.initial_state = {
            "zone": None,
            "public_port": None,
            "use_ssl": False,
        }
        self.content.update(self.initial_state)


class WebComponent(BaseComponent):
    kind = "web"

    def __init__(self, *args, **kwargs):
        keystore_passwords = kwargs.pop('keystore_passwords', [])
        super().__init__(*args, **kwargs)
        self.initial_state = {
            "keystore_passwords": keystore_passwords
        }
        self.content.update(self.initial_state)


class LauncherComponent(BaseComponent):
    kind = "launcher"

    def __init__(self, *args, **kwargs):
        connection_filter = kwargs.pop('connection_filter', None)
        super().__init__(*args, **kwargs)
        self.initial_state = {
            "connection_filter": connection_filter,
        }
        self.content.update(self.initial_state)


class ComponentRegistry(ZooKeeperBase):
    """A component registry is organized like:

        /zuul/components/{kind}/{sequence}

    Where kind corresponds to one of the classes in this module, and
    sequence is a ZK sequence node with a prefix of the hostname in
    order to produce a unique id for multiple identical components
    running on the same host.  An example path:

        /zuul/components/scheduler/hostname0000000000

    Components are ephemeral nodes, and so if the actual service
    disconnects from ZK, the node will disappear.

    Component objects returned by this class are read-only; updating
    their attributes will not be reflected in ZooKeeper.
    """
    log = logging.getLogger("zuul.ComponentRegistry")

    COMPONENT_CLASSES = {
        "scheduler": SchedulerComponent,
        "executor": ExecutorComponent,
        "merger": MergerComponent,
        "fingergw": FingerGatewayComponent,
        "web": WebComponent,
        "launcher": LauncherComponent,
    }

    def __init__(self, client):
        super().__init__(client)

        self.callbacks = set()
        self.client = client
        self._component_tree = None
        # kind -> hostname -> component
        self._cached_components = defaultdict(dict)

        self.model_api = None
        # Have we initialized enough to trust the model_api
        self._init = False
        # If we are already connected when the class is instantiated, directly
        # call the onConnect callback.
        if self.client.connected:
            self._onConnect()

    def waitForComponent(self, wait_component):
        while True:
            for kind, components in self.all():
                if wait_component.kind != kind:
                    continue
                for component in components:
                    if component.hostname == wait_component.hostname:
                        return

    def _getComponentRoot(self, kind):
        return '/'.join([COMPONENTS_ROOT, kind])

    def _getComponentPath(self, kind, hostname):
        return '/'.join([COMPONENTS_ROOT, kind, hostname])

    def _onConnect(self):
        for kind in self.COMPONENT_CLASSES.keys():
            root = self._getComponentRoot(kind)
            self.kazoo_client.ensure_path(root)
            self.kazoo_client.ChildrenWatch(
                root, self._makeComponentRootWatcher(kind))
        self._init = True
        self._updateMinimumModelApi()

    def _makeComponentRootWatcher(self, kind):
        def watch(children, event=None):
            return self._onComponentRootUpdate(kind, children, event)
        return watch

    def _onComponentRootUpdate(self, kind, children, event):
        for hostname in children:
            component = self._cached_components[kind].get(hostname)
            if not component:
                self.log.info("Noticed new %s component %s", kind, hostname)
                root = self._getComponentPath(kind, hostname)
                self.kazoo_client.DataWatch(
                    root, self._makeComponentWatcher(kind, hostname))

    def _makeComponentWatcher(self, kind, hostname):
        def watch(data, stat, event=None):
            return self._onComponentUpdate(kind, hostname, data, stat, event)
        return watch

    def _onComponentUpdate(self, kind, hostname, data, stat, event):
        if event:
            etype = event.type
        else:
            etype = None
        self.log.debug(
            "Registry %s got event %s for %s %s",
            self, etype, kind, hostname)
        if (etype in (None, EventType.CHANGED, EventType.CREATED) and
            data is not None):

            # Perform an in-place update of the cached component (if any)
            component = self._cached_components.get(kind, {}).get(hostname)
            d = json.loads(data.decode("utf-8"))

            self.log.info(
                "Component %s %s updated: %s",
                kind, hostname, d)

            if component:
                if (stat.version <= component._zstat.version):
                    # Don't update to older data
                    return
                component.updateFromDict(d)
                component._zstat = stat
            else:
                # Create a new component from the data structure
                # Get the correct kind of component
                # TODO (felix): KeyError on unknown component type?
                component_cls = self.COMPONENT_CLASSES[kind]
                # Pass in null ZK client to make a read-only component
                # instance.
                component = component_cls.fromDict(None, hostname, d)
                component.path = self._getComponentPath(kind, hostname)
                component._zstat = stat

            self._cached_components[kind][hostname] = component
            self._updateMinimumModelApi()
            self._handleCallbacks()
        elif (etype == EventType.DELETED or data is None):
            self.log.info(
                "Noticed %s component %s disappeared",
                kind, hostname)
            try:
                del self._cached_components[kind][hostname]
            except KeyError:
                # If it's already gone, don't care
                pass
            self._updateMinimumModelApi()
            self._handleCallbacks()
            # Return False to stop the datawatch
            return False

    def all(self, kind=None):
        """Returns a list of components.

        If kind is None, then a list of tuples is returned, with each tuple
        being (kind, [list of components]).

        :arg kind str: The type of component to look up in the registry, or
            None to return all kinds
        """

        if kind is None:
            return [(kind, list(components.values()))
                    for (kind, components) in
                    list(self._cached_components.items())]

        # Filter the cached components for the given kind
        return self._cached_components.get(kind, {}).values()

    def getMinimumModelApi(self):
        """Get the minimum model API version of all currently connected
        components"""

        # Start with our own version in case we're the only component
        # and we haven't registered.
        version = MODEL_API
        for kind, components in self.all():
            for component in components:
                version = min(version, component.model_api)
        return version

    def _updateMinimumModelApi(self):
        if not self._init:
            return
        version = self.getMinimumModelApi()
        if version != self.model_api:
            self.log.info(f"System minimum data model version {version}; "
                          f"this component {MODEL_API}")
        if self.model_api is None:
            if version < MODEL_API:
                self.log.info("The data model version of this component is "
                              "newer than the rest of the system; this "
                              "component will operate in compatability mode "
                              "until the system is upgraded")
            elif version > MODEL_API:
                self.log.error("The data model version of this component is "
                               "older than the rest of the system; "
                               "exiting to prevent data corruption")
                sys.exit(1)
        else:
            if version > self.model_api:
                if version > MODEL_API:
                    self.log.info("The data model version of this component "
                                  "is older than other components in the "
                                  "system, so other components will operate "
                                  "in a compability mode; upgrade this "
                                  "component as soon as possible to complete "
                                  "the system upgrade")
                elif version == MODEL_API:
                    self.log.info("The rest of the system has been upgraded "
                                  "to the data model version of this "
                                  "component")
            elif version < self.model_api:
                self.log.error("A component with a data model version older "
                               "than the rest of the system has been started; "
                               "data corruption is very likely to occur.")
                # Should we exit here as well?
        self.model_api = version

    def _handleCallbacks(self):
        for cb in list(self.callbacks):
            try:
                cb()
            except Exception:
                self.log.exception("Error in component registry callback:")

    def registerCallback(self, func):
        self.callbacks.add(func)

    def unregisterCallback(self, func):
        self.callbacks.discard(func)
