# Copyright 2021-2022 Acme Gating, LLC
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

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import abc
import contextlib
import json
import logging
import sys
import time
import types
import zlib
import collections

from kazoo.exceptions import (
    LockTimeout,
    NoNodeError,
    NodeExistsError,
    NotEmptyError,
)
from kazoo.retry import KazooRetry

from zuul.zk import sharding
from zuul.zk import ZooKeeperClient
from zuul.zk.exceptions import LockException
from zuul.zk.locks import SessionAwareLock


class BaseZKContext:
    profile_logger = logging.getLogger('zuul.profile')
    profile_default = False
    default_lock_identifier = None
    # Only changed by unit tests.
    # The default scales with number of procs.
    _max_workers = None

    def __init__(self):
        # We create the executor dict in enter to make sure that this
        # is used as a context manager and cleaned up properly.
        self.executor = None

    def __enter__(self):
        if self.executor:
            raise RuntimeError("ZKContext entered multiple times")
        # This is a dictionary keyed by class.  ZKObject subclasses
        # can request a dedicated ThreadPoolExecutor for their class
        # so that deserialize methods that use it can avoid deadlocks
        # with child class deserialize methods.
        self.executor = collections.defaultdict(
            lambda: ThreadPoolExecutor(
                max_workers=self._max_workers,
                thread_name_prefix="ZKContext",
            ))
        return self

    def __exit__(self, etype, value, tb):
        if self.executor:
            for executor in self.executor.values():
                if sys.version_info >= (3, 9):
                    executor.shutdown(wait=False, cancel_futures=True)
                else:
                    executor.shutdown(wait=False)
        self.executor = None


class ZKContext(BaseZKContext):
    def __init__(self, zk_client, lock, stop_event, log,
                 default_lock_identifier=None):
        super().__init__()
        if isinstance(zk_client, ZooKeeperClient):
            client = zk_client.client
        else:
            client = zk_client
        self.client = client
        self.lock = lock
        self.stop_event = stop_event
        self.log = log
        self.cumulative_read_time = 0.0
        self.cumulative_write_time = 0.0
        self.cumulative_read_objects = 0
        self.cumulative_write_objects = 0
        self.cumulative_read_znodes = 0
        self.cumulative_write_znodes = 0
        self.cumulative_read_bytes = 0
        self.cumulative_write_bytes = 0
        self.build_references = False
        self.profile = self.profile_default
        self.default_lock_identifier = default_lock_identifier

    def sessionIsValid(self):
        return (not self.lock or self.lock.is_still_valid())

    def sessionIsInvalid(self):
        return not self.sessionIsValid()

    def shouldAbortRetry(self):
        return (self.sessionIsInvalid() or
                (self.stop_event and self.stop_event.is_set()))

    def updateStatsFromOtherContext(self, other):
        self.cumulative_read_time += other.cumulative_read_time
        self.cumulative_write_time += other.cumulative_write_time
        self.cumulative_read_objects += other.cumulative_read_objects
        self.cumulative_write_objects += other.cumulative_write_objects
        self.cumulative_read_znodes += other.cumulative_read_znodes
        self.cumulative_write_znodes += other.cumulative_write_znodes
        self.cumulative_read_bytes += other.cumulative_read_bytes
        self.cumulative_write_bytes += other.cumulative_write_bytes

    def profileEvent(self, etype, path):
        if not self.profile:
            return
        self.profile_logger.debug(
            'ZK 0x%x %s %s  '
            'rt=%s wt=%s  ro=%s wo=%s  rn=%s wn=%s  rb=%s wb=%s',
            id(self), etype, path,
            self.cumulative_read_time, self.cumulative_write_time,
            self.cumulative_read_objects, self.cumulative_write_objects,
            self.cumulative_read_znodes, self.cumulative_write_znodes,
            self.cumulative_read_bytes, self.cumulative_write_bytes)


class LocalZKContext(BaseZKContext):
    """A Local ZKContext that means don't actually write anything to ZK"""

    def __init__(self, log):
        super().__init__()
        self.client = None
        self.lock = None
        self.stop_event = None
        self.log = log

    def sessionIsValid(self):
        return True

    def sessionIsInvalid(self):
        return False


class ZKObject:
    _retry_interval = 5
    _zkobject_compressed_size = 0
    _zkobject_uncompressed_size = 0
    io_reader_class = sharding.RawZKIO
    io_writer_class = sharding.RawZKIO
    truncate_on_create = False
    delete_on_error = False
    log_error_missing = True
    makepath = True

    # Implementations of these two methods are required
    def getPath(self):
        """Return the path to save this object in ZK

        :returns: A string representation of the Znode path
        """
        raise NotImplementedError()

    def serialize(self, context):
        """Implement this method to return the data to save in ZK.

        :returns: A byte string
        """
        raise NotImplementedError()

    # This should work for most classes
    def deserialize(self, data, context, extra=None):
        """Implement this method to convert serialized data into object
        attributes.

        :param bytes data: A byte string to deserialize
        :param ZKContext context: A ZKContext object with the current
            ZK session and lock.
        :param extra dict: A dictionary of extra data for use in
            deserialization.

        :returns: A dictionary of attributes and values to be set on
        the object.

        """
        if isinstance(data, dict):
            return data
        return json.loads(data.decode('utf-8'))

    # These methods are public and shouldn't need to be overridden
    def updateAttributes(self, context, **kw):
        """Update attributes on this object and save to ZooKeeper

        Instead of using attribute assignment, call this method to
        update attributes on this object.  It will update the local
        values and also write out the updated object to ZooKeeper.

        :param ZKContext context: A ZKContext object with the current
            ZK session and lock.  Be sure to acquire the lock before
            calling methods on this object.  This object will validate
            that the lock is still valid before writing to ZooKeeper.

        All other parameters are keyword arguments which are
        attributes to be set.  Set as many attributes in one method
        call as possible for efficient network use.
        """
        old = self.__dict__.copy()
        self._set(**kw)
        serial = self._trySerialize(context)
        if hash(serial) != getattr(self, '_zkobject_hash', None):
            try:
                self._save(context, serial)
            except Exception:
                # Roll back our old values if we aren't able to update ZK.
                self._set(**old)
                raise

    @contextlib.contextmanager
    def activeContext(self, context):
        if self._active_context:
            raise RuntimeError(
                f"Another context is already active {self._active_context}")
        try:
            old = self.__dict__.copy()
            self._set(_active_context=context)
            yield
            serial = self._trySerialize(context)
            if hash(serial) != getattr(self, '_zkobject_hash', None):
                try:
                    self._save(context, serial)
                except Exception:
                    # Roll back our old values if we aren't able to update ZK.
                    self._set(**old)
                    raise
        finally:
            self._set(_active_context=None)

    @classmethod
    def new(klass, context, **kw):
        """Create a new instance and save it in ZooKeeper"""
        obj = klass()
        obj._set(**kw)
        data = obj._trySerialize(context)
        obj._save(context, data, create=True)
        return obj

    @classmethod
    def fromZK(klass, context, path, **kw):
        """Instantiate a new object from data in ZK"""
        obj = klass()
        obj._set(**kw)
        obj._load(context, path=path)
        return obj

    def internalCreate(self, context):
        """Create the object in ZK from an existing ZKObject

        This should only be used in special circumstances: when we
        know it's safe to start using a ZKObject before it's actually
        created in ZK.  Normally use .new()
        """
        data = self._trySerialize(context)
        self._save(context, data, create=True)

    def refresh(self, context):

        """Update data from ZK"""
        self._load(context)

    def exists(self, context):
        """Return whether the object exists in ZK"""
        path = self.getPath()
        return bool(context.client.exists(path))

    def _trySerialize(self, context):
        if isinstance(context, LocalZKContext):
            return b''
        try:
            return self.serialize(context)
        except Exception:
            # A higher level must handle this exception, but log
            # ourself here so we know what object triggered it.
            context.log.error(
                "Exception serializing ZKObject %s", self)
            raise

    @classmethod
    def _delete(cls, context, path):
        if context.sessionIsInvalid():
            raise Exception("ZooKeeper session or lock not valid")
        cls._retry(context, context.client.delete,
                   path, recursive=True)
        context.profileEvent('delete', path)

    def delete(self, context):
        path = self.getPath()
        try:
            self._delete(context, path)
        except Exception:
            context.log.error(
                "Exception deleting ZKObject %s at %s", self, path)
            raise

    def estimateDataSize(self, seen=None):
        """Attempt to find all ZKObjects below this one and sum their
        compressed and uncompressed sizes.

        :returns: (compressed_size, uncompressed_size)
        """
        compressed_size = self._zkobject_compressed_size
        uncompressed_size = self._zkobject_uncompressed_size

        if seen is None:
            seen = {self}

        def walk(obj):
            compressed = 0
            uncompressed = 0
            if isinstance(obj, ZKObject):
                if obj in seen:
                    return 0, 0
                seen.add(obj)
                compressed, uncompressed = obj.estimateDataSize(seen)
            elif (isinstance(obj, dict) or
                  isinstance(obj, types.MappingProxyType)):
                for sub in list(obj.values()):
                    c, u = walk(sub)
                    compressed += c
                    uncompressed += u
            elif (isinstance(obj, list) or
                  isinstance(obj, tuple)):
                for sub in obj:
                    c, u = walk(sub)
                    compressed += c
                    uncompressed += u
            return compressed, uncompressed

        c, u = walk(self.__dict__)
        compressed_size += c
        uncompressed_size += u

        return (compressed_size, uncompressed_size)

    def getZKVersion(self):
        """Return the ZK version of the object as of the last load/refresh.

        Returns None if the object is newly created.
        """
        zstat = getattr(self, '_zstat', None)
        # If zstat is None, we created the object
        if zstat is None:
            return None
        return zstat.version

    def getZKczxid(self):
        """Return the ZK create transaction id of the object.

        Returns None if the object is newly created.
        """
        zstat = getattr(self, '_zstat', None)
        # If zstat is None, we created the object
        if zstat is None:
            return None
        return zstat.czxid

    def getZKmzxid(self):
        """Return the ZK last modified transaction id of the object as
        of the last load/refresh.

        Returns None if the object is newly created.

        """
        zstat = getattr(self, '_zstat', None)
        # If zstat is None, we created the object
        if zstat is None:
            return None
        return zstat.mzxid

    @staticmethod
    def getZKObjectMetadata(data):
        # Only ExpanableZKObjects implement this
        return None

    # Private methods below

    @classmethod
    def _retry(cls, context, func, *args, max_tries=-1, **kw):
        kazoo_retry = KazooRetry(max_tries=max_tries,
                                 interrupt=context.shouldAbortRetry,
                                 delay=cls._retry_interval, backoff=0,
                                 ignore_expire=False)
        try:
            return kazoo_retry(func, *args, **kw)
        except InterruptedError:
            pass

    def __init__(self):
        # Don't support any arguments in constructor to force us to go
        # through a save or restore path.
        super().__init__()
        self._set(
            _active_context=None,
            _zkobject_metadata=None,
        )

    @classmethod
    def _getIOReadStreamFactory(cls, context, path):
        def factory():
            return cls.io_reader_class(context.client, path)
        return factory

    def _getIOWriteStreamFactory(self, context, path, create, version):
        def factory():
            return self.io_writer_class(context.client, path,
                                        create=create,
                                        makepath=self.makepath,
                                        version=version)
        return factory

    @staticmethod
    def _retryableLoad(context, io_factory):
        with io_factory() as stream:
            compressed_data = stream.read()
            zstat = stream.zstat
        context.cumulative_read_time += stream.cumulative_read_time
        context.cumulative_read_objects += 1
        context.cumulative_read_znodes += stream.znodes_read
        context.cumulative_read_bytes += stream.bytes_read
        return compressed_data, zstat

    def _load(self, context, path=None):
        if path is None:
            path = self.getPath()
        compressed_data, zstat = self._loadData(context, path)
        self._updateFromRaw(context, compressed_data, zstat, None)

    @classmethod
    def _loadData(cls, context, path):
        if context.sessionIsInvalid():
            raise Exception("ZooKeeper session or lock not valid")
        try:
            io_factory = cls._getIOReadStreamFactory(context, path)
            compressed_data, zstat = cls._retry(context, cls._retryableLoad,
                                                context, io_factory)
            context.profileEvent('get', path)
        except Exception as exc:
            if cls.log_error_missing or not isinstance(exc, NoNodeError):
                context.log.error(
                    "Exception loading ZKObject at %s", path)
            if cls.delete_on_error:
                cls._delete(context, path)
            raise
        return compressed_data, zstat

    @classmethod
    def _fromRaw(cls, context, raw_data, zstat, extra, **kw):
        obj = cls()
        obj._updateFromRaw(context, raw_data, zstat, extra)
        return obj

    def _updateFromRaw(self, context, raw_data, zstat, extra):
        try:
            self._set(_zkobject_hash=None)
            metadata, data = self._decompressData(raw_data)
            self._set(**self.deserialize(data, context, extra))
            self._set(_zstat=zstat,
                      _zkobject_metadata=metadata,
                      _zkobject_hash=hash(data),
                      _zkobject_compressed_size=len(raw_data),
                      _zkobject_uncompressed_size=len(data))
        except Exception:
            if self.delete_on_error:
                self.delete(context)
            raise

    @classmethod
    def _compressData(cls, data):
        return zlib.compress(data)

    @classmethod
    def _decompressData(cls, raw_data):
        # Return metadata, data
        # metadata is only used by ExpandableZKObject
        try:
            return None, zlib.decompress(raw_data)
        except zlib.error:
            # Fallback for old, uncompressed data
            return None, raw_data

    @staticmethod
    def _retryableSave(context, io_factory, data):
        zstat = None
        with io_factory() as stream:
            stream.truncate(0)
            stream.write(data)
            stream.flush()
            context.cumulative_write_time += stream.cumulative_write_time
            context.cumulative_write_objects += 1
            context.cumulative_write_znodes += stream.znodes_written
            context.cumulative_write_bytes += stream.bytes_written
            zstat = stream.zstat
        return zstat

    def _save(self, context, data, create=False):
        if isinstance(context, LocalZKContext):
            return
        path = self.getPath()
        if context.sessionIsInvalid():
            raise Exception("ZooKeeper session or lock not valid")
        compressed_data = self._compressData(data)

        try:
            if create and not self.truncate_on_create:
                exists = self._retry(context, context.client.exists, path)
                context.profileEvent('exists', path)
                if exists is not None:
                    raise NodeExistsError
            zstat = getattr(self, '_zstat', None)
            if zstat is not None:
                version = self._zstat.version
            else:
                version = -1
            io_factory = self._getIOWriteStreamFactory(
                context, path, create, version)
            zstat = self._retry(context, self._retryableSave,
                                context, io_factory, compressed_data)
            context.profileEvent('set', path)
        except Exception:
            context.log.error(
                "Exception saving ZKObject %s at %s", self, path)
            raise
        self._set(_zstat=zstat,
                  _zkobject_hash=hash(data),
                  _zkobject_compressed_size=len(compressed_data),
                  _zkobject_uncompressed_size=len(data),
                  )

    def __setattr__(self, name, value):
        if self._active_context:
            super().__setattr__(name, value)
        else:
            raise Exception("Unable to modify ZKObject %s" %
                            (repr(self),))

    def _set(self, **kw):
        for name, value in kw.items():
            super().__setattr__(name, value)


class ShardedZKObject(ZKObject):
    # If the node exists when we create we normally error, unless this
    # is set, in which case we proceed and truncate.
    truncate_on_create = False
    # Normally we delete nodes which have syntax errors, but the
    # pipeline summary is read without a write lock, so those are
    # expected.  Don't delete them in that case.
    delete_on_error = True
    io_reader_class = sharding.BufferedShardReader
    io_writer_class = sharding.BufferedShardWriter


class LockableZKObjectMixin(abc.ABC):
    _lock = None
    _deleted = False

    def __new__(klass, *args, **kwargs):
        zko = super().__new__(klass)
        zko._set(_lock_contenders=dict())
        return zko

    def getLockPath(self):
        """Return the path for the lock of this object in ZK

        :returns: A string representation of the Znode path
        """
        raise NotImplementedError()

    @classmethod
    def new(klass, context, **kw):
        """Create a new instance and save it in ZooKeeper"""
        obj = klass()
        obj._set(**kw)
        # Create the lock path first.  In the future, if we want to
        # support creating locked objects, we can acquire it here.
        obj._createLockPath(context)
        data = obj._trySerialize(context)
        obj._save(context, data, create=True)
        return obj

    def internalCreate(self, context):
        self._createLockPath(context)
        super().internalCreate(context)

    def _createLockPath(self, context):
        if isinstance(context, LocalZKContext):
            return
        path = self.getLockPath()
        if context.sessionIsInvalid():
            raise Exception("ZooKeeper session or lock not valid")
        try:
            self._retry(context, context.client.ensure_path, path)
        except Exception:
            context.log.error(
                "Exception creating ZKObject %s lock path at %s", self, path)
            raise

    def _deleteLockPath(self, client):
        path = self.getLockPath()
        # Give other actors 30 seconds to realize this object
        # doesn't exist anymore and they should stop trying to
        # lock it.
        for x in range(30):
            # This handles connection-related retries, but not
            # conflicts.  We ignore the session here because we're
            # trying to delete a lock; we want to do that even if we
            # lose whatever lock the context might have.
            kazoo_retry = KazooRetry(max_tries=-1,
                                     delay=self._retry_interval,
                                     backoff=0)
            try:
                return kazoo_retry(client.delete, path, recursive=True)
            except NotEmptyError:
                time.sleep(1)

    @contextmanager
    def locked(self, context, blocking=True, timeout=None):
        if not (lock := self.acquireLock(context, blocking=blocking,
                                         timeout=timeout)):
            raise LockException(f"Failed to acquire lock on {self}")
        try:
            yield lock
        finally:
            try:
                self.releaseLock(context)
            except Exception:
                context.log.exception("Failed to release lock on %s", self)

    def acquireLock(self, context, blocking=True, timeout=None,
                    identifier=None):
        have_lock = False
        lock = None
        path = self.getLockPath()
        identifier = identifier or context.default_lock_identifier
        try:
            # We create the lock path when we create the object in ZK,
            # so there is no need to ensure the path on lock.  This
            # lets us avoid re-creating the lock if the object was
            # deleted behind our back.
            lock = SessionAwareLock(context.client, path,
                                    identifier=identifier,
                                    ensure_path=False)
            have_lock = lock.acquire(blocking, timeout)
        except NoNodeError:
            # Request disappeared
            have_lock = False
        except LockTimeout:
            have_lock = False
            context.log.error("Timeout trying to acquire lock: %s", path)

        # If we aren't blocking, it's possible we didn't get the lock
        # because someone else has it.
        if not have_lock:
            return None

        self._set(_lock=lock)
        return lock

    def releaseLock(self, context):
        if self._lock is None:
            return
        self._lock.release()
        self._set(_lock=None)
        if self._deleted:
            # If we are releasing the lock after deleting the object,
            # also cleanup the lock path.
            try:
                self._deleteLockPath(context.client)
            except Exception:
                context.log.error("Unable to delete lock path for %s", self)

    def delete(self, context):
        super().delete(context)
        self._set(_deleted=True)
        # Most lockable zkobjects are deleted while holding the lock,
        # so we let releaseLock delete the lock path.  But for those
        # that aren't, we should delete the lock path now.
        if not self.hasLock():
            try:
                self._deleteLockPath(context.client)
            except Exception:
                context.log.error("Unable to delete lock path for %s", self)

    def hasLock(self):
        if self._lock is None:
            return False
        return self._lock.is_still_valid()


class LockableZKObject(LockableZKObjectMixin, ZKObject):
    pass


class PolymorphicZKObjectMixin(abc.ABC):

    # Make this mixin a true abstract base class that can't be
    # instantiated. The '_subclass_id' property is automatically
    # "implemented" through subclassing (see '__init_subclass__').
    @property
    @abc.abstractmethod
    def _subclass_id(self):
        pass

    # Mapping of subclass id to subclass for a particular abstract
    # parent class. Creating an instance without a subclass id
    # starts a new hierarchy. This is only supported when none
    # of the parent classes is alread a variable ZKObject class.
    @property
    @abc.abstractmethod
    def _subclasses(self):
        pass

    def __init_subclass__(cls, *, subclass_id=None, **kwargs):
        super().__init_subclass__(**kwargs)
        if subclass_id is None:
            if isinstance(cls._subclasses, dict):
                raise TypeError(
                    "Can't create new hierarchy below existing variable base")
            # This is a new "base" class
            cls._subclasses = {}
        else:
            if not isinstance(cls._subclasses, dict):
                raise TypeError(
                    "Can't create a subclass without variable base class")
            # "Implement" the abstract '_subclass_id' property
            sid = subclass_id.encode("utf8")
            if sid in cls._subclasses:
                raise ValueError(
                    f"Subclass with id {subclass_id} already exists")
            cls._subclass_id = sid
            cls._subclasses[sid] = cls

    @classmethod
    def fromZK(cls, context, path, **kw):
        raw_data, zstat = cls._loadData(context, path)
        return cls._fromRaw(context, raw_data, zstat, None, **kw)

    @classmethod
    def _compressData(cls, data):
        compressed_data = super()._compressData(data)
        return b"\0".join((cls._subclass_id, compressed_data))

    @classmethod
    def _decompressData(cls, raw_data):
        subclass_id, _, compressed_data = raw_data.partition(b"\0")
        return super()._decompressData(compressed_data)

    @classmethod
    def _fromRaw(cls, context, raw_data, zstat, extra, **kw):
        subclass_id, _, _ = raw_data.partition(b"\0")
        try:
            klass = cls._subclasses[subclass_id]
        except KeyError:
            raise RuntimeError(f"Unknown subclass id: {subclass_id}")
        return super(
            PolymorphicZKObjectMixin, klass)._fromRaw(
                context, raw_data, zstat, extra, **kw)


class ExpandableZKObject(ZKObject):
    # If the node exists when we create we normally error, unless this
    # is set, in which case we proceed and truncate.
    truncate_on_create = False
    # Normally we delete nodes which have syntax errors.
    delete_on_error = True
    io_reader_class = sharding.RawExpandableIO
    io_writer_class = sharding.RawExpandableIO

    @classmethod
    def _getIOReadStreamFactory(cls, context, path):
        def factory():
            metadata = sharding.ObjectMetadata()
            return cls.io_reader_class(context.client, path,
                                       metadata=metadata)
        return factory

    def _getIOWriteStreamFactory(self, context, path, create, version):
        def factory():
            return self.io_writer_class(context.client, path,
                                        create=create,
                                        makepath=self.makepath,
                                        version=version,
                                        metadata=self._zkobject_metadata)

        return factory

    @staticmethod
    def getZKObjectMetadata(data):
        # Return the metadata and the remainder
        return sharding.ObjectMetadata.fromRaw(data)


class ExpandableLockableZKObject(LockableZKObjectMixin, ExpandableZKObject):
    pass
