# Copyright 2018 Red Hat, Inc.
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

import re


DEPENDS_ON_RE = re.compile(r"^Depends-On:(.*?)\s*$",
                           re.MULTILINE | re.IGNORECASE)


def find_dependency_headers(message):
    # Search for Depends-On headers
    dependencies = []
    for match in DEPENDS_ON_RE.findall(message):
        if match in dependencies:
            continue
        dependencies.append(match.strip())
    return dependencies


def expand_dependencies(description, ref_map, baseurl):
    """Expand shorthand Depends-On values to full URLs.

    :param str description: Raw description text.
    :param dict ref_map: Mapping of shorthand text to href.
    :param str baseurl: Base URL to prepend to relative hrefs.
    :return: The description with shorthand values expanded.
    :rtype: str
    """
    def _replace(match):
        value = match.group(1).strip()
        if '://' in value or value not in ref_map:
            return match.group(0)
        href = ref_map[value]
        if href.startswith('/'):
            href = baseurl + href
        return match.group(0).replace(value, href)

    return DEPENDS_ON_RE.sub(_replace, description)
