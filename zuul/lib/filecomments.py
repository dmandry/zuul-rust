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

import voluptuous as vs


FILE_COMMENT = {
    'line': int,
    'message': str,
    'range': {
        'start_line': int,
        'start_character': int,
        'end_line': int,
        'end_character': int,
    },
    'level': str,
}

FILE_COMMENTS = {str: [FILE_COMMENT]}

FILE_COMMENTS_SCHEMA = vs.Schema(FILE_COMMENTS)


def validate(file_comments):
    FILE_COMMENTS_SCHEMA(file_comments)


def extractLines(file_comments):
    """Extract file/line tuples from file comments for mapping"""

    lines = set()
    for path, comments in file_comments.items():
        for comment in comments:
            if 'line' in comment:
                lines.add((path, int(comment['line'])))
            if 'range' in comment:
                rng = comment['range']
                for key in ['start_line', 'end_line']:
                    if key in rng:
                        lines.add((path, int(rng[key])))
    return list(lines)


def updateLines(file_comments, lines):
    """Update the line numbers in file_comments with the supplied mapping"""

    for path, comments in file_comments.items():
        for comment in comments:
            if 'line' in comment:
                comment['line'] = lines.get((path, comment['line']),
                                            comment['line'])
            if 'range' in comment:
                rng = comment['range']
                for key in ['start_line', 'end_line']:
                    if key in rng:
                        rng[key] = lines.get((path, rng[key]), rng[key])


class LineMapper:
    hunk_re = re.compile(r'^@@ -\d+,\d+ \+(\d+),(\d+) @@$')

    def __init__(self, diff_output):
        hunk_start = None
        hunk_range = None
        hunk_line = None
        offsets = []
        last_offset = None
        for l in diff_output.split('\n'):
            if len(l) == 0:
                continue
            m = self.hunk_re.match(l)
            if m:
                hunk_start = int(m.group(1))
                hunk_range = int(m.group(2))
                hunk_line = 0
                continue
            if not hunk_start:
                continue
            if hunk_line > hunk_range:
                # We have somehow run off the end of the hunk;
                # shouldn't happen.
                hunk_start = None
                continue
            if l[0] == ' ':
                last_offset = None
                hunk_line += 1
                continue
            if not last_offset:
                last_offset = [(hunk_start + hunk_line), 0]
                offsets.append(last_offset)
            if l[0] == '+':
                last_offset[1] -= 1
            elif l[0] == '-':
                last_offset[1] += 1
        self.offsets = offsets

    def mapLine(self, lineno):
        new_lineno = lineno
        for (start, offset) in self.offsets:
            if lineno > start:
                new_lineno += offset
        return new_lineno
