# Copyright 2021 Red Hat
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

# NOTE(ianw): 2021-08-30 we are testing whitespace things that trigger
# flake8, for now there is no way to turn of specific tests on a
# per-file basis.

# flake8: noqa

import textwrap

from zuul.lib.dependson import find_dependency_headers, expand_dependencies

from tests.base import BaseTestCase


class TestDependsOnParsing(BaseTestCase):

    def test_depends_on_parsing(self):
        msg = textwrap.dedent('''\
        This is a sample review subject

        Review text

        Depends-On:https://this.is.a.url/1
        Depends-On: https://this.is.a.url/2  
        Depends-On:    https://this.is.a.url/3
        Depends-On:	  https://this.is.a.url/4	
        ''')
        r = find_dependency_headers(msg)

        self.assertListEqual(r,
                             ['https://this.is.a.url/1',
                              'https://this.is.a.url/2',
                              'https://this.is.a.url/3',
                              'https://this.is.a.url/4'])

    def test_expand_dependencies(self):
        """Test expand_dependencies substitution logic.

        Verifies three scenarios in a single description:
        - A full URL Depends-On is left unchanged.
        - A shorthand ref present in ref_map is expanded to a
          full URL using baseurl + href.
        - A shorthand ref not in ref_map is left unchanged.
        """
        description = textwrap.dedent('''\
        Review text

        Depends-On: https://gitlab.example.com/org/project/-/merge_requests/1
        Depends-On: org/project!2
        Depends-On: org/other!999
        ''')
        ref_map = {
            'org/project!2': '/org/project/-/merge_requests/2',
        }
        result = expand_dependencies(
            description, ref_map, 'https://gitlab.example.com')
        expected = textwrap.dedent('''\
        Review text

        Depends-On: https://gitlab.example.com/org/project/-/merge_requests/1
        Depends-On: https://gitlab.example.com/org/project/-/merge_requests/2
        Depends-On: org/other!999
        ''')
        self.assertEqual(result, expected)
