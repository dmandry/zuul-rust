# Copyright 2023 Acme Gating, LLC
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
import boto3
import logging
import pprint
import threading
import time

from zuul.zk.event_queues import EventReceiverElection, EventCheckpoint

# Kinesis sort of looks like Kafka, but has some important differences:
# We poll over HTTP (of course).

# We get a shard iterator for every shard in the Kinesis stream.  That
# keeps our position in the shard as we iterate over the chunks of
# data that AWS returns.  Each time we poll, we get a new shard
# iterator.  However, the iterator is only valid for 5 minutes, so we
# can't use it for checkpointing.

# The docs recommend running a thread for each shard and continuously
# polling with a 1 second delay.  We're going to run an election for
# each shard and let the schedulers fight it out.

# We can use the sequence number to checkpoint, so if we record that
# in ZK, when Zuul recovers it can start at the last sequence number.
# However, that may have an indeterminate, potentially large, series
# of empty Kinesis data chunks between it and any newer records.


class GerritAWSKinesisEventListener:
    log = logging.getLogger("zuul.GerritConnection.awskinesis")

    def __init__(self, gerrit_connection, connection_config):
        self.gerrit_connection = gerrit_connection
        region = connection_config.get('aws_kinesis_region')
        access_key = connection_config.get('aws_kinesis_access_key')
        secret_key = connection_config.get('aws_kinesis_secret_key')
        self.stream = connection_config.get('aws_kinesis_stream', 'gerrit')
        args = dict(
            region_name=region,
        )
        if access_key:
            args['aws_access_key_id'] = access_key
            args['aws_secret_access_key'] = secret_key
        self.client = boto3.client('kinesis', **args)
        self._event_count = 0  # Only for unit tests
        self._caught_up = {}  # Only for unit tests
        self.init()

    def init(self):
        # This is in a separate method so the unit tests can restart
        # the listener.
        gerrit_connection = self.gerrit_connection
        stream_info = self.client.describe_stream(StreamName=self.stream)
        self.shard_ids = []
        self.elections = {}
        self.checkpoints = {}
        self._threads = []
        for shard in stream_info['StreamDescription']['Shards']:
            sid = shard['ShardId']
            self.shard_ids.append(sid)
            self.elections[sid] = EventReceiverElection(
                gerrit_connection.sched.zk_client,
                gerrit_connection.connection_name,
                f"aws_kinesis_{sid}")
            self.checkpoints[sid] = EventCheckpoint(
                gerrit_connection.sched.zk_client,
                gerrit_connection.connection_name,
                f"aws_kinesis_{sid}")
            self._threads.append(threading.Thread(
                target=self.run, args=(sid,), daemon=True))
            self._caught_up[sid] = False
        self._stop_event = threading.Event()
        self._stopped = False

    def start(self):
        for thread in self._threads:
            thread.start()

    def stop(self):
        self.log.debug("Stopping AWS Kineses listener")
        self._stopped = True
        self._stop_event.set()

    def join(self):
        for thread in self._threads:
            thread.join()

    def _run(self, shard_id):
        self.log.info("Starting shard consumer for shard %s",
                      shard_id)

        # Were we caught up in the last iteration of the loop
        last_caught_up = False
        checkpoint = self.checkpoints[shard_id]
        last_seen_sequence_no = checkpoint.get()

        # Arguments to get a shard iterator
        args = dict(
            StreamName=self.stream,
            ShardId=shard_id,
        )

        # Determine what kind of iterator to get based on whether we
        # have checkpoint data.
        if last_seen_sequence_no is None:
            args['ShardIteratorType'] = 'LATEST'
            self.log.debug("Shard %s starting with latest event", shard_id)
        else:
            args['ShardIteratorType'] = 'AFTER_SEQUENCE_NUMBER'
            args['StartingSequenceNumber'] = last_seen_sequence_no
            self.log.debug("Shard %s starting after sequence number %s",
                           shard_id, last_seen_sequence_no)

        try:
            response = self.client.get_shard_iterator(**args)
        except Exception:
            self.log.exception("Error obtaining shard %s iterator, "
                               "retrying from latest",
                               shard_id)
            # Retry from latest (ignoring our checkpoint; it may be
            # too old, or the user deleted the data)
            args['ShardIteratorType'] = 'LATEST'
            args.pop('StartingSequenceNumber', None)

            # If it fails again only asking for latest, it's fatal
            response = self.client.get_shard_iterator(**args)

        shard_iterator = response['ShardIterator']

        while not self._stopped:
            # The most recently read sequence number in this batch
            sequence_no = None
            response = self.client.get_records(
                ShardIterator=shard_iterator,
            )
            shard_iterator = response['NextShardIterator']
            time_behind = response['MillisBehindLatest']
            if time_behind:
                self.log.debug(
                    "Shard %s received %s records and is %sms behind",
                    shard_id, len(response['Records']), time_behind)
                # We're behind, so poll a little more frequently to
                # catch up faster
                delay = 0.5
                last_caught_up = False
            else:
                if not last_caught_up:
                    # Only emit this log once each time we catch up.
                    self.log.debug(
                        "Shard %s received %s records and is caught up",
                        shard_id, len(response['Records']))
                    self._caught_up[shard_id] = True
                last_caught_up = True
                delay = 1.0

            for record in response['Records']:
                sequence_no = record['SequenceNumber']
                data = json.loads(record['Data'].decode('utf8'))
                self.log.info("Received data from kinesis: \n%s" %
                              pprint.pformat(data))
                self.gerrit_connection.addEvent(data)
                self._event_count += 1

            if sequence_no is not None:
                self.log.debug("Shard %s setting sequence number %s",
                               shard_id, sequence_no)
                checkpoint.set(sequence_no)
            time.sleep(delay)

    def run(self, shard_id):
        while not self._stopped:
            try:
                self.elections[shard_id].run(self._run, shard_id)
            except Exception:
                self.log.exception(
                    "Exception in AWS Kinesis consumer shard %s with %s:",
                    shard_id, self.gerrit_connection.connection_name)
                self._stop_event.wait(5)
