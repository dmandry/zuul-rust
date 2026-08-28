# Copyright 2025 Acme Gating, LLC
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


import json
import sys


def main():
    console_path = sys.argv[1]
    output_txt_path = sys.argv[2]
    output_json_path = sys.argv[3]
    with open(output_json_path) as f:
        output_json = json.loads(f.read())
    with open(output_txt_path) as f:
        output_txt = f.read()
    with open(console_path) as f:
        console = f.read()

    # Get a list of all the task ids where we skippod log live
    # streaming (but we get one for each host, so use a set to dedup).
    skipped = set()

    for playbook in output_json:
        for play in playbook['plays']:
            for task in play['tasks']:
                for host in task['hosts'].values():
                    logid = host.get('zuul_log_id')
                    if logid == 'skip':
                        skipped.add(task['task']['id'])
                        # The only tasks that should do this are:
                        assert "command should not stream" in host['stdout']

    # We should have skipped 2 tasks
    assert len(skipped) == 2
    for skip in skipped:
        # Verify that we did not attempt a connection
        assert f"Starting to log {skip}" not in console

    # We should have output due to copying from the result dict
    assert "This command should not stream 1" in output_txt
    assert "This command should not stream 2" in output_txt


if __name__ == "__main__":
    main()
