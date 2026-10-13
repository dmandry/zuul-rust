# Copyright 2023-2026 Acme Gating, LLC
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

import threading
import weakref


class ChangeNetworkFuture:
    """A set of changes related by dependencies"""
    def __init__(self, min_ltime):
        self.changes = set()
        self.event = threading.Event()
        self.query_results = {}
        # We will not accept cached data older than this time
        self.min_ltime = min_ltime

    def add(self, change_key):
        self.changes.add(change_key)

    def setComplete(self):
        self.event.set()

    def wait(self):
        self.event.wait()

    def hasNumber(self, number):
        for change_key in self.changes:
            if number == change_key.stable_id:
                return True
        return False

    def addQueryResult(self, number, data):
        existing = self.query_results.get(number)
        existing_time = existing and existing.zuul_query_ltime or 0
        if existing_time < data.zuul_query_ltime:
            self.query_results[number] = data

    def getQueryResult(self, number):
        data = self.query_results.get(number)
        return data

    def mergeQueryResults(self, future):
        for (number, data) in future.query_results.items():
            if number not in self.query_results:
                self.query_results[number] = data
            else:
                own_data = self.query_results[number]
                if own_data.zuul_query_ltime < data.zuul_query_ltime:
                    self.query_results[number] = data


class ChangeNetworkConflict(Exception):
    """Raised when two threads are found to be walking the same change graph.

    The associated ChangeNetworkFuture is the winner of the conflict
    and the caller should wait for it.

    """

    def __init__(self, future):
        self.future = future


class ChangeNetworkManager:
    """Prevent collisions between multiple threads querying changes"""

    def __init__(self):
        self.lock = threading.Lock()
        self.futures = []
        self.query_locks = weakref.WeakValueDictionary()

    def getQueryLock(self, number):
        # TODO: verify that WeakValueDictionary.setdefault is atomic;
        # if so, we can remove this internal locking call.
        with self.lock:
            return self.query_locks.setdefault(number, threading.Lock())

    def getQueryResult(self, number):
        with self.lock:
            for f in self.futures:
                if f.hasNumber(number):
                    data = f.getQueryResult(number)
                    if data:
                        return data

    def updateQueryResult(self, number, data):
        with self.lock:
            for f in self.futures:
                if f.hasNumber(number):
                    f.addQueryResult(number, data)

    def permissionToProceed(self, future):
        with self.lock:
            for f in self.futures:
                if f is future:
                    continue
                if future.changes.intersection(f.changes):
                    # The first thread to request permission for a
                    # change always succeeds, second or later threads
                    # connected to the same network always lose.
                    if future in self.futures:
                        self.futures.remove(future)
                    # Let the winner benefit from any queries the
                    # loser has performed.
                    f.mergeQueryResults(future)
                    raise ChangeNetworkConflict(f)
            # No conflicts
            if future not in self.futures:
                self.futures.append(future)
        return True

    def setComplete(self, future):
        if future is None:
            return
        with self.lock:
            try:
                self.futures.remove(future)
            except ValueError:
                pass
            future.setComplete()
