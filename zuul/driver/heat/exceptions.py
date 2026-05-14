# Copyright 2026 Red Hat, Inc.
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

import textwrap

from zuul.exceptions import ConfigurationSyntaxError


class HeatTemplateSecurityError(ConfigurationSyntaxError):
    zuul_error_name = 'Heat Template Security Error'

    def __init__(self, path, nodeset, detail):
        message = textwrap.dedent("""\
        Heat template "{path}" in nodeset "{nodeset}" failed a
        security check: {detail}""")
        message = textwrap.fill(message.format(
            path=path, nodeset=nodeset, detail=detail))
        super().__init__(message)


class HeatTemplateNotFoundError(ConfigurationSyntaxError):
    zuul_error_name = 'Heat Template Not Found'

    def __init__(self, path, nodeset):
        message = textwrap.dedent("""\
        Heat template "{path}" referenced in nodeset "{nodeset}"
        was not found in the repository.""")
        message = textwrap.fill(message.format(
            path=path, nodeset=nodeset))
        super().__init__(message)


class FlavorNotFoundError(Exception):
    """Raised when a Nova flavor cannot be found."""
    pass


class FlavorLimitExceeded(Exception):
    """Raised when a template exceeds flavor resource limits."""
    pass
