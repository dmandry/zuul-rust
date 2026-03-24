# Copyright 2022 Acme Gating, LLC
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

# Analyze the contents of the ZK tree (whether in ZK or a dump on the
# local filesystem) to identify large objects.

import argparse
import json
import os
import sys
import zlib

import kazoo.client


KB = 1024
MB = 1024**2
GB = 1024**3


def convert_human(size):
    if size >= GB:
        return f'{int(size / GB)}G'
    if size >= MB:
        return f'{int(size / MB)}M'
    if size >= KB:
        return f'{int(size / KB)}K'
    if size > 0:
        return f'{size}B'
    return '0'


def convert_null(size):
    return size


def unconvert_human(size):
    suffix = size[-1]
    val = size[:-1]
    if suffix in ['G', 'g']:
        return int(val) * GB
    if suffix in ['M', 'm']:
        return int(val) * MB
    if suffix in ['K', 'k']:
        return int(val) * KB
    return int(size)


class SummaryLine:
    def __init__(self, kind, path, size=0, zk_size=0):
        self.kind = kind
        self.path = path
        self.size = size
        self.zk_size = zk_size
        self.attrs = {}
        self.children = []

    @property
    def tree_size(self):
        return sum([x.tree_size for x in self.children] + [self.size])

    @property
    def zk_tree_size(self):
        return sum([x.zk_tree_size for x in self.children] + [self.zk_size])

    def add(self, child):
        self.children.append(child)

    def __str__(self):
        indent = 0
        return self.toStr(indent)

    def matchesLimit(self, limit, zk):
        if not limit:
            return True
        if zk:
            size = self.zk_size
        else:
            size = self.size
        if size >= limit:
            return True
        for child in self.children:
            if child.matchesLimit(limit, zk):
                return True
        return False

    def toStr(self, indent, depth=None, conv=convert_null, limit=0, zk=False):
        """Convert this item and its children to a str representation

        :param indent int: How many levels to indent
        :param depth int: How many levels deep to display
        :param conv func: A function to convert sizes to text
        :param limit int: Don't display items smaller than this
        :param zk bool: Whether to use the data size (False)
                        or ZK storage size (True)
        """
        if depth and indent >= depth:
            return ''
        if self.matchesLimit(limit, zk):
            attrs = ' '.join([f'{k}={conv(v)}' for k, v in self.attrs.items()])
            if attrs:
                attrs = ' ' + attrs
            if zk:
                size = conv(self.zk_size)
                tree_size = conv(self.zk_tree_size)
            else:
                size = conv(self.size)
                tree_size = conv(self.tree_size)
            ret = ('  ' * indent + f"{self.kind} {self.path} "
                   f"size={size} tree={tree_size}{attrs}\n")
            for child in self.children:
                ret += child.toStr(indent + 1, depth, conv, limit, zk)
        else:
            ret = ''
        return ret


class Data:
    def __init__(self, path, raw, zk_size=None, failed=False):
        self.path = path
        self.raw = raw
        self.failed = failed
        self.zk_size = zk_size or len(raw)
        if not failed:
            self.data = json.loads(raw)
        else:
            print(f"!!! {path} failed to load data")
            self.data = {}

    @property
    def size(self):
        return len(self.raw)


class Tree:
    def getNode(self, path):
        pass

    def listChildren(self, path):
        pass

    def listConnections(self):
        return self.listChildren('/zuul/cache/connection')

    def listBlobPrefixes(self):
        return self.listChildren('/zuul/cache/blob/data')

    def listBlobs(self, prefix):
        return self.listChildren(f'/zuul/cache/blob/data/{prefix}')

    def getBlob(self, prefix, blob_id):
        return self.getShardedNode(f'/zuul/cache/blob/data/{prefix}'
                                   f'/{blob_id}/data')

    def getBranchCache(self, connection):
        return self.getShardedNode(f'/zuul/cache/connection/{connection}'
                                   '/branches/data')

    def listCacheKeys(self, connection):
        return self.listChildren(f'/zuul/cache/connection/{connection}/cache')

    def getCacheKey(self, connection, key):
        return self.getNode(f'/zuul/cache/connection/{connection}/cache/{key}')

    def listCacheData(self, connection):
        return self.listChildren(f'/zuul/cache/connection/{connection}/data')

    def getCacheData(self, connection, key):
        return self.getShardedNode(f'/zuul/cache/connection/{connection}'
                                   f'/data/{key}')

    def listTenants(self):
        return self.listChildren('/zuul/tenant')

    def listPipelines(self, tenant):
        return self.listChildren(f'/zuul/tenant/{tenant}/pipeline')

    def getPipeline(self, tenant, pipeline):
        return self.getNode(f'/zuul/tenant/{tenant}/pipeline/{pipeline}')

    def getPipelineChangeList(self, pipeline):
        return self.getShardedNode(f'{pipeline}/change_list')

    def getPipelineStatus(self, pipeline):
        return self.getShardedNode(f'{pipeline}/change_list')

    def listQueues(self, pipeline):
        return self.listChildren(f'{pipeline}/queue')

    def getQueue(self, pipeline, queue_uuid):
        return self.getNode(f'{pipeline}/queue/{queue_uuid}')

    def listItems(self, pipeline):
        return self.listChildren(f'{pipeline}/item')

    def getItem(self, pipeline, item_uuid):
        return self.getNode(f'{pipeline}/item/{item_uuid}')

    def listBuildsets(self, item):
        return self.listChildren(f'{item}/buildset')

    def getBuildset(self, item, buildset):
        return self.getNode(f'{item}/buildset/{buildset}')

    def getFiles(self, buildset):
        return self.getShardedNode(f'{buildset}/files')

    def listJobs(self, buildset):
        return self.listChildren(f'{buildset}/job')

    def getJob(self, buildset, job_name):
        return self.getNode(f'{buildset}/job/{job_name}')

    def listBuilds(self, job):
        return self.listChildren(f'{job}/build')

    def getBuild(self, job, build_uuid):
        return self.getNode(f'{job}/build/{build_uuid}')

    def getJobDataAttribute(self, job, attribute):
        return self.getShardedNode(f'{job}/{attribute}')

    def getBuildData(self, build, data_name):
        return self.getShardedNode(f'{build}/{data_name}')


class FilesystemTree(Tree):
    def __init__(self, root):
        self.root = root

    def getNode(self, path):
        path = path.lstrip('/')
        fullpath = os.path.join(self.root, path)
        if not os.path.exists(fullpath):
            return Data(path, '', failed=True)
        try:
            with open(os.path.join(fullpath, 'ZKDATA'), 'rb') as f:
                zk_data = f.read()
                data = zk_data
                try:
                    data = zlib.decompress(zk_data)
                except Exception:
                    pass
                return Data(path, data, zk_size=len(zk_data))
        except Exception:
            return Data(path, '', failed=True)

    def getShardedNode(self, path):
        path = path.lstrip('/')
        fullpath = os.path.join(self.root, path)
        if not os.path.exists(fullpath):
            return Data(path, '', failed=True)
        shards = sorted([x for x in os.listdir(fullpath)
                         if x != 'ZKDATA'])
        data = b''
        compressed_data_len = 0
        try:
            for shard in shards:
                with open(os.path.join(fullpath, shard, 'ZKDATA'), 'rb') as f:
                    compressed_data = f.read()
                    compressed_data_len += len(compressed_data)
                    data += zlib.decompress(compressed_data)
            return Data(path, data, zk_size=compressed_data_len)
        except Exception:
            return Data(path, data, failed=True)

    def listChildren(self, path):
        path = path.lstrip('/')
        fullpath = os.path.join(self.root, path)
        if not os.path.exists(fullpath):
            return []
        return [x for x in os.listdir(fullpath)
                if x != 'ZKDATA']


class ZKTree(Tree):
    def __init__(self, host, cert, key, ca):
        kwargs = {}
        if cert:
            kwargs['use_ssl'] = True
            kwargs['keyfile'] = key
            kwargs['certfile'] = cert
            kwargs['ca'] = ca
        self.client = kazoo.client.KazooClient(host, **kwargs)
        self.client.start()

    def getNode(self, path):
        path = path.lstrip('/')
        if not self.client.exists(path):
            return Data(path, '', failed=True)
        try:
            zk_data, _ = self.client.get(path)
            data = zk_data
            try:
                data = zlib.decompress(zk_data)
            except Exception:
                pass
            return Data(path, data, zk_size=len(zk_data))
        except Exception:
            return Data(path, '', failed=True)

    def getShardedNode(self, path):
        path = path.lstrip('/')
        if not self.client.exists(path):
            return Data(path, '', failed=True)
        shards = sorted(self.listChildren(path))
        data = b''
        compressed_data_len = 0
        try:
            for shard in shards:
                compressed_data, _ = self.client.get(os.path.join(path, shard))
                compressed_data_len += len(compressed_data)
                data += zlib.decompress(compressed_data)
            return Data(path, data, zk_size=compressed_data_len)
        except Exception:
            return Data(path, data, failed=True)

    def listChildren(self, path):
        path = path.lstrip('/')
        try:
            return self.client.get_children(path)
        except kazoo.client.NoNodeError:
            return []


class Analyzer:
    def __init__(self, args):
        if args.path:
            self.tree = FilesystemTree(args.path)
        else:
            self.tree = ZKTree(args.host, args.cert, args.key, args.ca)
        if args.depth is not None:
            self.depth = int(args.depth)
        else:
            self.depth = None
        if args.human:
            self.conv = convert_human
        else:
            self.conv = convert_null
        if args.limit:
            self.limit = unconvert_human(args.limit)
        else:
            self.limit = 0
        self.use_zk_size = args.zk_size

    def summarizeItems(self, pipeline):
        for item_uuid in self.tree.listItems(pipeline.path):
            item = self.tree.getItem(pipeline.path, item_uuid)
            item_summary = SummaryLine(
                'Item', item.path, item.size, item.zk_size)

            known_children = {"buildset"}
            children = set(self.tree.listChildren(item.path))
            if unknown := children - known_children:
                print(f"Unknown child node(s): {unknown} @{item.path}")

            buildsets = self.tree.listBuildsets(item.path)
            for bs_i, bs_id in enumerate(buildsets):
                # Add each buildset
                buildset = self.tree.getBuildset(item.path, bs_id)
                buildset_summary = SummaryLine(
                    'Buildset', buildset.path,
                    buildset.size, buildset.zk_size)
                item_summary.add(buildset_summary)

                known_children = {"files", "job"}
                children = set(self.tree.listChildren(buildset.path))
                if unknown := children - known_children:
                    print(f"Unknown child node(s): {unknown} @{buildset.path}")

                # Files
                if "files" in children:
                    files = self.tree.getFiles(buildset.path)
                    files_summary = SummaryLine(
                        'Files', files.path, files.size, files.zk_size)
                    buildset_summary.add(files_summary)

                jobs = self.tree.listJobs(buildset.path)
                for job_i, job_uuid in enumerate(jobs):
                    # Add each job
                    job = self.tree.getJob(buildset.path, job_uuid)
                    job_summary = SummaryLine('Job', job.path,
                                              job.size, job.zk_size)
                    buildset_summary.add(job_summary)

                    known_children = {"build"}
                    children = set(self.tree.listChildren(job.path))
                    job_data_attributes = children - known_children

                    for attr_name in job_data_attributes:
                        attr = self.tree.getJobDataAttribute(
                            job.path, attr_name)
                        attr_summary = SummaryLine(
                            f'Attribute{attr_name.title()}', attr.path,
                            attr.size, attr.zk_size)
                        job_summary.add(attr_summary)

                    builds = self.tree.listBuilds(job.path)
                    for build_i, build_id in enumerate(builds):
                        # Add each build
                        build = self.tree.getBuild(job.path, build_id)
                        build_summary = SummaryLine(
                            'Build', build.path, build.size, build.zk_size)
                        job_summary.add(build_summary)

                        build_data_names = set(
                            self.tree.listChildren(build.path))
                        for data_name in build_data_names:
                            data = self.tree.getBuildData(
                                build.path, data_name)
                            data_summary = SummaryLine(
                                data_name.title().replace("_", ""),
                                data.path, data.size, data.zk_size)
                            build_summary.add(data_summary)

            yield item_summary

    def summarizePipelines(self):
        for tenant_name in self.tree.listTenants():
            for pipeline_name in self.tree.listPipelines(tenant_name):
                pipeline = self.tree.getPipeline(tenant_name, pipeline_name)
                pipeline_summary = SummaryLine(
                    'Pipeline', pipeline.path, pipeline.size, pipeline.zk_size)

                known_children = {
                    "change_list", "status", "item", "queue", "dirty"
                }
                children = set(self.tree.listChildren(pipeline.path))
                if unknown := children - known_children:
                    print(f"Unknown child node(s): {unknown} @{pipeline.path}")

                # Change List
                change_list = self.tree.getPipelineChangeList(pipeline.path)
                change_list_summary = SummaryLine(
                    'ChangeList', change_list.path, change_list.size,
                    change_list.zk_size)
                pipeline_summary.add(change_list_summary)

                # Status
                status = self.tree.getPipelineStatus(pipeline.path)
                status_summary = SummaryLine(
                    'Status', status.path, status.size, status.zk_size)
                pipeline_summary.add(status_summary)

                # Queues
                for queue_uuid in self.tree.listQueues(pipeline.path):
                    queue = self.tree.getQueue(pipeline.path, queue_uuid)
                    queue_summary = SummaryLine(
                        'Queue', queue.path, queue.size, queue.zk_size)
                    pipeline_summary.add(queue_summary)

                # Items
                for item_summary in self.summarizeItems(pipeline):
                    pipeline_summary.add(item_summary)

                sys.stdout.write(
                    pipeline_summary.toStr(
                        0, self.depth, self.conv, self.limit, self.use_zk_size
                    ))

    def summarizeConnectionCache(self, connection_name):
        connection_summary = SummaryLine('Connection', connection_name, 0, 0)
        branch_cache = self.tree.getBranchCache(connection_name)
        branch_summary = SummaryLine(
            'Branch Cache', connection_name,
            branch_cache.size, branch_cache.zk_size)
        connection_summary.add(branch_summary)

        cache_key_summary = SummaryLine(
            'Change Cache Keys', connection_name, 0, 0)
        cache_key_summary.attrs['count'] = 0
        connection_summary.add(cache_key_summary)
        for key in self.tree.listCacheKeys(connection_name):
            cache_key = self.tree.getCacheKey(connection_name, key)
            cache_key_summary.size += cache_key.size
            cache_key_summary.zk_size += cache_key.zk_size
            cache_key_summary.attrs['count'] += 1

        cache_data_summary = SummaryLine(
            'Change Cache Data', connection_name, 0, 0)
        cache_data_summary.attrs['count'] = 0
        connection_summary.add(cache_data_summary)
        for key in self.tree.listCacheData(connection_name):
            cache_data = self.tree.getCacheData(connection_name, key)
            cache_data_summary.size += cache_data.size
            cache_data_summary.zk_size += cache_data.zk_size
            cache_data_summary.attrs['count'] += 1

        sys.stdout.write(connection_summary.toStr(
            0, self.depth, self.conv, self.limit, self.use_zk_size))

    def summarizeConnections(self):
        for connection_name in self.tree.listConnections():
            self.summarizeConnectionCache(connection_name)

    def summarizeBlobStore(self):
        blob_summary = SummaryLine('Blob', '/zuul/cache/blob', 0, 0)
        blob_summary.attrs['count'] = 0
        for prefix in self.tree.listBlobPrefixes():
            for blob_id in self.tree.listBlobs(prefix):
                blob = self.tree.getBlob(prefix, blob_id)
                blob_summary.size += blob.size
                blob_summary.zk_size += blob.zk_size
                blob_summary.attrs['count'] += 1

        sys.stdout.write(blob_summary.toStr(
            0, self.depth, self.conv, self.limit, self.use_zk_size))

    def summarize(self):
        self.summarizeConnections()
        self.summarizePipelines()
        self.summarizeBlobStore()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--path',
                        help='Filesystem path for previously dumped data')
    parser.add_argument('--host',
                        help='ZK host string (exclusive with --path)')
    parser.add_argument('--cert', help='Path to TLS certificate')
    parser.add_argument('--key', help='Path to TLS key')
    parser.add_argument('--ca', help='Path to TLS CA cert')
    parser.add_argument('-d', '--depth', help='Limit depth when printing')
    parser.add_argument('-H', '--human', dest='human', action='store_true',
                        help='Use human-readable sizes')
    parser.add_argument('-l', '--limit', dest='limit',
                        help='Only print nodes greater than limit')
    parser.add_argument('-Z', '--zksize', dest='zk_size', action='store_true',
                        help='Use the possibly compressed ZK storage size '
                        'instead of plain data size')
    args = parser.parse_args()

    az = Analyzer(args)
    az.summarize()
