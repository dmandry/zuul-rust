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

import re


KEY_RE = re.compile('^[A-Za-z0-9-]+:.+')


def parse_commit_message(message):
    # Return a tuple with the parts of a commit message:
    # (body, trailers); each of those as lists of lines.
    body = []
    trailers = []
    lines = message.split('\n')
    orig_lines = lines.copy()
    # Examine each line in reverse order since the interesting part is
    # the last paragraph.
    while lines:
        line = lines.pop()
        if not line.strip():
            if trailers:
                # As soon as we see a blank line before a
                # non-blank line, we have found the last
                # paragraph.  If it starts with a trailer, it's a
                # trailer block, otherwise there is no trailer
                # block.
                if KEY_RE.match(trailers[0]):
                    body = lines + [line]
                    return body, trailers
                else:
                    return orig_lines, []
        else:
            # Only consider non-blank lines in the trailer block
            trailers.insert(0, line)
    return orig_lines, []
