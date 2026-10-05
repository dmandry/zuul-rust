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

from zuul.lib.gitutil import parse_commit_message

from tests.base import BaseTestCase


class TestCommitMessageParser(BaseTestCase):

    # We normally use textwrap.dedent, but it alters whitespace too
    # much for our testing here.
    def test_normal_message(self):
        msg = """\
Commit message subject

First paragraph
has multiple lines.

Second paragraph
also has multiple lines.

Signed-Off-By: foo
"""

        body, trailers = parse_commit_message(msg)
        self.assertEqual([
            'Commit message subject',
            '',
            'First paragraph',
            'has multiple lines.',
            '',
            'Second paragraph',
            'also has multiple lines.',
            '',
        ], body)
        self.assertEqual([
            'Signed-Off-By: foo',
        ], trailers)

    def test_extra_whitespace(self):
        msg = """\
Commit message subject

First paragraph
has multiple lines.

Second paragraph
also has multiple lines.
\t
Signed-Off-By: foo
"""

        body, trailers = parse_commit_message(msg)
        self.assertEqual([
            'Commit message subject',
            '',
            'First paragraph',
            'has multiple lines.',
            '',
            'Second paragraph',
            'also has multiple lines.',
            '\t',
        ], body)
        self.assertEqual([
            'Signed-Off-By: foo',
        ], trailers)

    def test_subject_only(self):
        msg = """\
Commit message subject

Signed-Off-By: foo
Signed-Off-By: bar"""

        body, trailers = parse_commit_message(msg)
        self.assertEqual([
            'Commit message subject',
            '',
        ], body)
        self.assertEqual([
            'Signed-Off-By: foo',
            'Signed-Off-By: bar',
        ], trailers)

    def test_no_trailers(self):
        msg = """\
Commit message subject

First paragraph
has multiple lines.

Second paragraph
also has multiple lines.
"""

        body, trailers = parse_commit_message(msg)
        self.assertEqual([
            'Commit message subject',
            '',
            'First paragraph',
            'has multiple lines.',
            '',
            'Second paragraph',
            'also has multiple lines.',
            ''
        ], body)
        self.assertEqual([], trailers)

    def test_invalid_trailer_block(self):
        msg = """\
Commit message subject

First paragraph
has multiple lines.
Signed-Off-By: foo
"""

        body, trailers = parse_commit_message(msg)
        self.assertEqual([
            'Commit message subject',
            '',
            'First paragraph',
            'has multiple lines.',
            'Signed-Off-By: foo',
            ''
        ], body)
        self.assertEqual([], trailers)

    def test_empty_message(self):
        msg = ""
        body, trailers = parse_commit_message(msg)
        self.assertEqual([''], body)
        self.assertEqual([], trailers)

    def test_no_empty_lines1(self):
        msg = """\
Commit message subject
"""

        body, trailers = parse_commit_message(msg)
        self.assertEqual([
            'Commit message subject',
            '',
        ], body)
        self.assertEqual([], trailers)

    def test_no_empty_lines2(self):
        msg = "Commit message subject"

        body, trailers = parse_commit_message(msg)
        self.assertEqual([
            'Commit message subject',
        ], body)
        self.assertEqual([], trailers)
