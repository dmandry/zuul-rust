# Copyright 2022 Acme Gating, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import uuid

from zuul.lib.logutil import get_annotated_logger

import cherrypy
from cherrypy._cplogging import LogManager


def set_request_id(request):
    if not hasattr(request, 'zuul_request_id'):
        lowercase_headers = dict(
            (k.lower(), request.headers[k]) for k in request.headers.keys()
        )
        # github webhook
        if "x-github-delivery" in lowercase_headers:
            request.zuul_request_id = lowercase_headers["x-github-delivery"]
            return
        # gitlab webhook
        elif "x-gitlab-event-uuid" in lowercase_headers:
            request.zuul_request_id = lowercase_headers["x-gitlab-event-uuid"]
            return
        else:
            request.zuul_request_id = uuid.uuid4().hex
    return

class ZuulCherrypyLogManager(LogManager):
    @property
    def access_log(self):
        request = cherrypy.serving.request
        set_request_id(request)
        return get_annotated_logger(self._access_log,
                                    request=request.zuul_request_id)

    @access_log.setter
    def access_log(self, attr):
        self._access_log = attr

    @property
    def error_log(self):
        request = cherrypy.serving.request
        if not hasattr(request, 'zuul_request_id'):
            request.zuul_request_id = uuid.uuid4().hex
        return get_annotated_logger(self._error_log,
                                    request=request.zuul_request_id)

    @error_log.setter
    def error_log(self, attr):
        self._error_log = attr
