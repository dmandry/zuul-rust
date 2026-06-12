# Copyright 2024, 2026 Acme Gating, LLC
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

import json
import argparse
import os
import subprocess
import logging
import sys

parser = argparse.ArgumentParser(
    description="Replay the Zuul workspace repo setup using information "
    "saved from a Zuul build's workspace-repos.json file.")
parser.add_argument('file', help='Path to workspace-repos.json')
parser.add_argument('-v', action='store_true', help='Verbose logging')
parser.add_argument('--workspace', help='Path to workspace')
options = parser.parse_args()

if options.v:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)


class CloneException(Exception):
    pass


class Workspace:
    def __init__(self, path):
        self.path = path
        self.log = logging.getLogger('workspace')

    def run(self, cmd, cwd, timestamp=None, vardict=None,
            result=None, level=logging.INFO):
        if vardict is None:
            vardict = {}
        # Perform any variable replacements requested
        new_cmd = []
        for token in cmd:
            if token.startswith('$'):
                token = vardict.get(token[1:], token)
            new_cmd.append(token)
        self.log.log(level, 'Run: "%s"', ' '.join(cmd))
        env = self.env
        if timestamp:
            env = env.copy()
            env['GIT_COMMITTER_DATE'] = str(int(timestamp)) + '+0000'
            env['GIT_AUTHOR_DATE'] = str(int(timestamp)) + '+0000'
        if result:
            capture_output = True
        else:
            capture_output = False
        out = subprocess.run(new_cmd, cwd=cwd, env=env, check=True,
                             capture_output=capture_output)
        if result:
            vardict[result] = out.stdout.decode('utf8').strip()

    def load(self, fn):
        with open(fn) as f:
            data = json.load(f)
        self.repo_state = data['repo_state']
        self.merge_ops = data['merge_ops']
        self.merge_name = data['merge_name']
        self.merge_email = data['merge_email']
        self.env = {
            'GIT_AUTHOR_NAME': self.merge_name,
            'GIT_AUTHOR_EMAIL': self.merge_email,
            'GIT_COMITTER_NAME': self.merge_name,
            'GIT_COMITTER_EMAIL': self.merge_email,
        }

    def clone(self):
        for repo, state in self.repo_state.items():
            path = os.path.join(self.path, repo)
            fail = False
            if not os.path.exists(path):
                self.log.error("Please clone the repo %s", repo)
                fail = True
            if fail:
                raise CloneException()

    def reset(self):
        for repo, state in self.repo_state.items():
            path = os.path.join(self.path, repo)
            for ref, sha in state.items():
                self.run(['git', 'update-ref', ref, sha],
                         cwd=path, level=logging.DEBUG)

    def replay(self):
        vardict = {}
        for op in self.merge_ops:
            if 'cmd' in op:
                self.run(op['cmd'],
                         cwd=os.path.join(self.path, op['path']),
                         timestamp=op.get('timestamp'),
                         vardict=vardict,
                         result=op.get('result'),
                         )
            if 'comment' in op:
                self.log.info(op['comment'])


w = Workspace(options.workspace or os.getcwd())
w.load(options.file)
try:
    w.clone()
except CloneException:
    sys.exit(1)
w.reset()
w.replay()
