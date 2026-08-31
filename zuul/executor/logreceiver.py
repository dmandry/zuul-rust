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

import os
import logging
import multiprocessing
import socketserver
import threading
import json
from zuul.executor.fileutil import _safe_open
from zuul.lib.logutil import get_annotated_logger

# This class is in its own module to minimize what the multiprocessing
# module needs to provide when we launch the subprocess.


class LogReceiver:
    # The syslog format combines facility and severity into a priority
    # code.  We don't need to reverse that; since we know the
    # combinations we're using, we just map the constants here.
    # TODO: in the future we can shift the result output to this
    # system via a different facility code.
    OUTPUT_PRIORITY = '<135>'
    JSON_PRIORITY = '<134>'
    TYPE_STOP = 0
    TYPE_MESSAGE = 1
    TYPE_JSON = 2
    TYPE_SECRETS = 3
    TYPE_SYNC = 4

    def __init__(self, zuul_event_id, build, jobdir, stop_conn, behavior):
        # Avoid storing the jobdir itself to minimize subprocess resources
        self.jobdir_log_socket = jobdir.log_socket
        self.jobdir_job_output_file = jobdir.job_output_file
        self.jobdir_json_output_file = jobdir.json_output_file
        self.build = build
        self.log = get_annotated_logger(
            logging.getLogger("zuul.ExecutorServer"),
            zuul_event_id, build=build)
        self.parent_conn, self.child_conn = multiprocessing.Pipe()
        self.name = f"log-{build}"
        self.proc = multiprocessing.Process(target=self.run,
                                            name=self.name)
        self.server = None
        self.job_output_file = None
        self.stop_conn = stop_conn
        self.secrets = []
        self.behavior = behavior

    def start(self, cgroup_manager):
        self.log.debug("Starting log receiver at %s", self.jobdir_log_socket)
        self.proc.start()
        if cgroup_manager:
            cgroup_manager.moveToBuildGroup(self.proc.pid)
        # Wait for the child to send None to tell us it's started up
        self.parent_conn.recv()

    def _stopJob(self):
        self.stop_conn.send(self.build)

    def _containsSecrets(self, message):
        for secret in self.secrets:
            if secret in message:
                self.log.error("Secret detected in output")
                return True

    def _addLineToJobOutput(self, message):
        # Only used within the subprocess
        stop = False
        if self.behavior != 'ignore':
            if self._containsSecrets(message):
                message = "[Zuul] Secret detected in output"
                if self.behavior == 'fail':
                    stop = True
        output = self.job_output_file
        output.write(message + '\n')
        output.flush()
        if stop:
            self._stopJob()
            return

    def _addPlaybookToJson(self, message):
        # Only used within the subprocess
        stop = False
        if self.behavior != 'ignore':
            if self._containsSecrets(message):
                try:
                    message = self._makeFakePlaybook(
                        message,
                        "[Zuul] Secret detected in output"
                    )
                except Exception:
                    self.log.exception("Error creating fake playbook")
                    return
                if self.behavior == 'fail':
                    stop = True
        output = self.json_output_file
        file_len = output.seek(0, os.SEEK_END)
        if file_len == 0:
            # This is a new file without any content
            output.write('[\n')
        else:
            # Existing file: Remove three bytes to eat the trailing
            # '\n]\n' and add the ',' on the end of the last line.
            output.seek(file_len - 3)
            output.write(',\n')
        output.write(message)
        output.write('\n]\n')
        output.flush()
        if stop:
            self._stopJob()
            return

    def _makeFakePlaybook(self, data, message):
        playbook_info = json.loads(data)
        playbook_info['plays'] = [{
            'play': {
                'name': message
            },
            'tasks': []
        }]
        data = json.dumps(playbook_info,
                          indent=4, sort_keys=True, separators=(',', ': '))
        return data

    def _setSecrets(self, message):
        self.secrets = json.loads(message)

    def addLineToJobOutput(self, message):
        self.parent_conn.send((self.TYPE_MESSAGE, message))

    def addPlaybookToJson(self, data):
        self.parent_conn.send((self.TYPE_JSON, data))

    def sendSecrets(self, secrets):
        secrets = json.dumps(secrets)
        self.parent_conn.send((self.TYPE_SECRETS, secrets))

    def _handlePacket(self, data):
        data = data.decode('utf8')
        try:
            if data[:5] == self.OUTPUT_PRIORITY:
                message = data[5:]
                self._addLineToJobOutput(message)
            elif data[:5] == self.JSON_PRIORITY:
                message = data[5:]
                self._addPlaybookToJson(message)
        except Exception:
            self.log.exception("Error in log receiver:")

    def run(self):
        # This is the main method of the subprocess
        self.log = multiprocessing.log_to_stderr(level=logging.WARNING)

        class MyHandler(socketserver.StreamRequestHandler):
            def handle(this):
                buf = bytearray()
                while b := this.rfile.read(1):
                    if b == b'\x00':
                        self._handlePacket(buf)
                        buf = bytearray()
                    else:
                        buf.extend(b)

        self.thread = threading.Thread(target=self.manage,
                                       name=f"manage-{self.name}")
        try:
            with _safe_open(self.jobdir_job_output_file, "a") as job_output:
                with (_safe_open(self.jobdir_json_output_file, "w") as
                      json_output):
                    self.job_output_file = job_output
                    self.json_output_file = json_output
                    self.thread.start()
                    with socketserver.ThreadingUnixStreamServer(
                            self.jobdir_log_socket,
                            MyHandler) as server:
                        self.server = server
                        self.child_conn.send(None)
                        self.server.serve_forever()
        except Exception:
            self.log.exception("Error in log receiver:")

    def manage(self):
        while True:
            kind, msg = self.child_conn.recv()
            if kind == self.TYPE_STOP:
                self._stop()
                return
            elif kind == self.TYPE_MESSAGE:
                self._addLineToJobOutput(msg)
            elif kind == self.TYPE_JSON:
                self._addPlaybookToJson(msg)
            elif kind == self.TYPE_SECRETS:
                self._setSecrets(msg)
            elif kind == self.TYPE_SYNC:
                self._sync()

    def _sync(self):
        # This uses an internal attribute; if it ever changes, we can
        # perform our own thread accounting using the handler methods;
        # but this is simpler for now.
        self.server._threads.join()
        self.child_conn.send((self.TYPE_SYNC, None))

    def sync(self):
        self.parent_conn.send((self.TYPE_SYNC, None))
        kind, msg = self.parent_conn.recv()

    def _stop(self):
        try:
            if self.server:
                # The server polls for shutown, so this can take up to
                # 0.5 seconds.
                self.server.shutdown()
                self.server.server_close()
        except Exception:
            self.log.exception('Unable to stop log receiver:')

    def stop(self):
        self.parent_conn.send((self.TYPE_STOP, None))
        self.proc.join(30)
        if self.proc.exitcode is None:
            self.log.error('Log receiver did not exit')
            try:
                self.proc.terminate()
            except Exception:
                self.log.exception("Error terminating log receiver")
