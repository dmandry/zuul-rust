# Copyright 2012 Hewlett-Packard Development Company, L.P.
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

import re
import urllib
import logging
import voluptuous as vs
from urllib.parse import urlparse
from zuul.source import BaseSource
from zuul.model import Project
from zuul.driver.gerrit.gerritmodel import GerritRefFilter, GerritChange
from zuul.driver.util import (
    scalar_or_list,
    ZUUL_REGEX,
)
from zuul.zk.change_cache import ChangeKey


def noneOrStr(val):
    if val is None:
        return None
    return str(val)


class GerritSource(BaseSource):
    name = 'gerrit'
    log = logging.getLogger("zuul.source.Gerrit")

    def __init__(self, driver, connection, config=None):
        hostname = connection.canonical_hostname
        super(GerritSource, self).__init__(driver, connection,
                                           hostname, config)
        prefix_ui = urlparse(self.connection.baseurl).path
        if prefix_ui:
            prefix_ui = prefix_ui.lstrip('/').rstrip('/')
            prefix_ui += '/'

        self.change_re = re.compile(
            r"/%s(\#\/c\/|c\/.*\/\+\/)?(\d+)[\w]*" % prefix_ui)

    def getRefSha(self, project, ref):
        return self.connection.getRefSha(project, ref)

    def isMerged(self, change, head=None):
        return self.connection.isMerged(change, head)

    def canMerge(self, change, allow_needs, event=None, allow_refresh=False):
        # Gerrit changes have no volatile data that cannot be updated via
        # events and thus needs not to act on allow_refresh.
        return self.connection.canMerge(change, allow_needs, event=event)

    def postConfig(self):
        pass

    def getChangeKey(self, event):
        connection_name = self.connection.connection_name
        if event.change_number:
            return ChangeKey(connection_name, None,
                             'GerritChange',
                             str(event.change_number),
                             noneOrStr(event.patch_number))
        revision = f'{event.oldrev}..{event.newrev}'
        if event.ref and event.ref.startswith('refs/tags/'):
            tag = event.ref[len('refs/tags/'):]
            return ChangeKey(connection_name, event.project_name,
                             'Tag', tag, revision)
        if event.ref and not event.ref.startswith('refs/'):
            # Pre 2.13 Gerrit ref-updated events don't have branch prefixes.
            return ChangeKey(connection_name, event.project_name,
                             'Branch', event.ref, revision)
        if event.ref and event.ref.startswith('refs/heads/'):
            # From the timer trigger or Post 2.13 Gerrit
            branch = event.ref[len('refs/heads/'):]
            return ChangeKey(connection_name, event.project_name,
                             'Branch', branch, revision)
        if event.ref:
            # catch-all ref (ie, not a branch or head)
            return ChangeKey(connection_name, event.project_name,
                             'Ref', event.ref, revision)
        self.log.warning("Unable to format change key for %s" % (event,))

    def getChange(self, change_key, refresh=False, event=None):
        return self.connection.getChange(change_key, refresh=refresh,
                                         event=event)

    def getProjectBranchSha(self, project, branch_name):
        return (
            self.connection.getRefSha(project, f'refs/heads/{branch_name}')
            or None
        )

    def getChangeByURL(self, url, event):
        try:
            parsed = urllib.parse.urlparse(url)
        except ValueError:
            return None
        path = parsed.path
        if parsed.fragment:
            path += '#' + parsed.fragment
        m = self.change_re.match(path)
        if not m:
            return None
        try:
            change_no = int(m.group(2))
        except ValueError:
            return None
        query = "change:%s" % (change_no,)
        results = self.connection.simpleQuery(query, event=event)
        if not results:
            return None
        change_key = ChangeKey(self.connection.connection_name, None,
                               'GerritChange',
                               str(results[0].number),
                               str(results[0].current_patchset))
        change = self.connection._getChange(change_key, event=event)
        return change

    def getChangesDependingOn(self, change, projects, tenant):
        changes = []
        if not change.uris:
            return changes
        queries = set()
        for uri in change.uris:
            queries.add('message:{Depends-On: %s}' % uri)
        query = '(' + ' OR '.join(queries) + ')'
        results = self.connection.simpleQuery(query)
        self.log.debug('%s possible depending changes found', len(results))
        seen = set()
        for result in results:
            c = GerritChange(None)
            c.update(result, {}, self.connection)
            for match in c.get_dependency_messages():
                found = False
                for uri in change.uris:
                    if uri in match:
                        found = True
                        break
                if not found:
                    continue
                key = (c.number, c.patchset)
                if key in seen:
                    continue
                seen.add(key)
                change_key = ChangeKey(self.connection.connection_name, None,
                                       'GerritChange',
                                       str(c.number),
                                       str(c.patchset))
                change = self.connection._getChange(change_key)
                changes.append(change)

        return changes

    def useDependenciesByTopic(self):
        return bool(self.connection.submit_whole_topic)

    def getChangesByTopic(self, topic, event=None, changes=None, history=None):
        if not topic:
            return []

        if changes is None:
            changes = {}

        if history is None:
            history = []
        history.append(topic)

        query = 'status:open topic:"%s"' % topic
        results = self.connection.simpleQuery(query)
        for result in results:
            change_key = ChangeKey(self.connection.connection_name, None,
                                   'GerritChange',
                                   str(result.number),
                                   str(result.current_patchset))
            if change_key in changes:
                continue

            change = self.connection._getChange(change_key, event=event)
            changes[change_key] = change

        # Convert to list here because the recursive call can mutate
        # the set.
        for change in list(changes.values()):
            for git_change_ref in change.git_needs_changes:
                change_key = ChangeKey.fromReference(git_change_ref)
                if change_key in changes:
                    continue
                git_change = self.getChange(change_key, event=event)
                if not git_change.topic or git_change.topic in history:
                    continue
                self.getChangesByTopic(
                    git_change.topic, event, changes, history)
        return list(changes.values())

    def getCachedChanges(self):
        yield from self.connection._change_cache

    def getProject(self, name):
        p = self.connection.getProject(name)
        if not p:
            p = Project(name, self)
            self.connection.addProject(p)
        return p

    def getProjectOpenChanges(self, project):
        return self.connection.getProjectOpenChanges(project)

    def getProjectDefaultMergeMode(self, project, valid_modes=None):
        # The gerrit jgit merge operation is most closely approximated
        # by "git merge -s resolve", so we return that as the default
        # for the Gerrit driver.
        return 'merge-resolve'

    def getProjectDefaultBranch(self, project, tenant, min_ltime=-1):
        # We have to return something here, so try to get it from the
        # cache, and if that fails, return the Zuul default.
        try:
            default_branch = self.connection.getProjectDefaultBranch(
                project, tenant, min_ltime)
        except Exception:
            default_branch = None
        if default_branch is None:
            return super().getProjectDefaultBranch(project, tenant, min_ltime)
        return default_branch

    def getProjectBranches(self, project, tenant, min_ltime=-1):
        return self.connection.getProjectBranches(project, tenant, min_ltime)

    def getProjectBranchCacheLtime(self):
        return self.connection._branch_cache.ltime

    def getGitUrl(self, project):
        return self.connection.getGitUrl(project)

    def _getGitwebUrl(self, project, sha=None):
        return self.connection._getGitwebUrl(project, sha)

    def getRequireFilters(self, config, parse_context):
        f = GerritRefFilter.requiresFromConfig(
            self.connection.connection_name,
            config, parse_context)
        return [f]

    def getRejectFilters(self, config, parse_context):
        f = GerritRefFilter.rejectFromConfig(
            self.connection.connection_name,
            config, parse_context)
        return [f]

    def getRefForChange(self, change):
        partial = str(change).zfill(2)[-2:]
        return "refs/changes/%s/%s/.*" % (partial, change)

    def setChangeAttributes(self, change, **attrs):
        return self.connection.updateChangeAttributes(change, **attrs)


approval = vs.Schema({'username': str,
                      'email': str,
                      'older-than': str,
                      'newer-than': str,
                      }, extra=vs.ALLOW_EXTRA)


def getRequireSchema():
    require = {'approval': scalar_or_list(approval),
               'open': bool,
               'current-patchset': bool,
               'wip': bool,
               'hashtags': scalar_or_list(vs.Any(ZUUL_REGEX, str)),
               'status': scalar_or_list(str)}
    return require


def getRejectSchema():
    reject = {'approval': scalar_or_list(approval),
              'open': bool,
              'current-patchset': bool,
              'wip': bool,
              'hashtags': scalar_or_list(vs.Any(ZUUL_REGEX, str)),
              'status': scalar_or_list(str)}
    return reject
