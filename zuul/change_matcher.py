# Copyright 2015 Red Hat, Inc.
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

"""
This module defines classes used in matching changes based on job
configuration.
"""

import json

from zuul.lib.re2util import ZuulRegex

COMMIT_MSG = '/COMMIT_MSG'


class AbstractChangeMatcher(object):
    """An abstract class that matches change attributes against regular
    expressions

    :arg ZuulRegex regex: A Zuul regular expression object to match

    """

    def __init__(self, regex):
        self._regex = regex.pattern
        self.regex = regex

    def matches(self, change):
        """Return a boolean indication of whether change matches
        implementation-specific criteria.
        """
        raise NotImplementedError()

    def copy(self):
        return self.__class__(self.regex)

    def __deepcopy__(self, memo):
        return self.copy()

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self.regex == other.regex)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(json.dumps(self.regex.toDict(), sort_keys=True))

    def __str__(self):
        return '{%s:%s}' % (self.__class__.__name__, self._regex)

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self._regex)


class ProjectMatcher(AbstractChangeMatcher):

    def matches(self, change):
        return self.regex.match(str(change.project))


class BranchMatcher(AbstractChangeMatcher):
    exactmatch = False

    def matchesBranch(self, branch):
        # an implied branch matcher must do a fullmatch to work correctly
        if self.exactmatch:
            if self._regex == branch:
                return True
        else:
            if self.regex.match(branch):
                return True
        return False

    def matches(self, change):
        if hasattr(change, 'branch'):
            return self.matchesBranch(change.branch)
        if self.regex.match(change.ref):
            return True
        if hasattr(change, 'containing_branches'):
            for branch in change.containing_branches:
                if self.exactmatch:
                    if self._regex == branch:
                        return True
                else:
                    if self.regex.match(branch):
                        return True
        return False

    def serialize(self):
        return {
            "implied": self.exactmatch,
            "regex": self.regex.serialize(),
        }

    @classmethod
    def deserialize(cls, data):
        regex = ZuulRegex.deserialize(data['regex'])
        o = cls(regex)
        return o


class ImpliedBranchMatcher(BranchMatcher):
    exactmatch = True


class FileMatcher(AbstractChangeMatcher):

    pass


class AbstractMatcherCollection(AbstractChangeMatcher):

    def __init__(self, matchers):
        self.matchers = matchers

    def __eq__(self, other):
        return str(self) == str(other)

    def __hash__(self):
        return hash(json.dumps(sorted([hash(x) for x in self.matchers])))

    def __str__(self):
        return '{%s:%s}' % (self.__class__.__name__,
                            ','.join([str(x) for x in self.matchers]))

    def __repr__(self):
        return '<%s [%s]>' % (self.__class__.__name__,
                              ', '.join([repr(x) for x in self.matchers]))

    def copy(self):
        return self.__class__(self.matchers[:])


class AbstractMatchFiles(AbstractMatcherCollection):

    @property
    def regexes(self):
        for matcher in self.matchers:
            yield matcher.regex


class MatchAllFiles(AbstractMatchFiles):

    def matches(self, change):
        # NOTE(yoctozepto): make irrelevant files matcher match when
        # there are no files to check - return False (NB: reversed)
        if not hasattr(change, 'files'):
            return False
        files = [c for c in change.files if c != COMMIT_MSG]
        if not files:
            return False
        for file_ in files:
            matched_file = False
            for regex in self.regexes:
                if regex.match(file_):
                    matched_file = True
                    break
            if not matched_file:
                return False
        return True


class MatchAnyFiles(AbstractMatchFiles):

    def matches(self, change):
        # NOTE(yoctozepto): make files matcher match when
        # there are no files to check - return True
        if not hasattr(change, 'files'):
            return True
        files = [c for c in change.files if c != COMMIT_MSG]
        if not files:
            return True
        for file_ in files:
            for regex in self.regexes:
                if regex.match(file_):
                    return True
        return False


class MatchAll(AbstractMatcherCollection):

    def matches(self, change):
        for matcher in self.matchers:
            if not matcher.matches(change):
                return False
        return True


class MatchAny(AbstractMatcherCollection):

    def matches(self, change):
        for matcher in self.matchers:
            if matcher.matches(change):
                return True
        return False
