# Copyright 2026 Acme Gating, LLC
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
import logging
import threading


class MonitoringThreadPoolExecutor(ThreadPoolExecutor):
    _zuul_log = logging.getLogger("zuul.MonitoringThreadPoolExecutor")

    def __init__(self, *args, **kw):
        self._zuul_current = 0
        self._zuul_lock = threading.Lock()
        self._zuul_statsd = kw.pop('statsd', None)
        self._zuul_metric = kw.pop('metric', None)
        self._zuul_emit()
        super().__init__(*args, **kw)

    def _zuul_emit(self):
        try:
            if self._zuul_statsd:
                self._zuul_statsd.gauge(
                    self._zuul_metric,
                    self._zuul_current,
                )
        except Exception:
            self._zuul_log.exception("Error emitting stat:")

    def submit(self, fn, /, *args, **kw):
        def wrap(*wrap_args, **wrap_kw):
            with self._zuul_lock:
                self._zuul_current += 1
                self._zuul_emit()
            try:
                return fn(*wrap_args, **wrap_kw)
            finally:
                with self._zuul_lock:
                    self._zuul_current -= 1
                    self._zuul_emit()
        return super().submit(wrap, *args, **kw)
