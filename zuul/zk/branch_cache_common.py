# Copyright 2014 Rackspace Australia
# Copyright 2021 BMW Group
# Copyright 2021, 2024 Acme Gating, LLC
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

# Everything in this file should be moved to branch_cache.py once we
# drop MODEL_API < 37.

from enum import IntFlag

# Default marker to raise an exception on cache miss in getProjectBranches()
RAISE_EXCEPTION = object()


# These flags should be the purview of the drivers, but we need to
# know about them in order to support backwards compatability to
# MODEL_API < 27.  In the future, we should be able to make these
# driver-specific and have driver-specific subclasses of BranchInfo,
# etc.
class BranchFlag(IntFlag):
    CLEAR = 0
    PRESENT = 0x1
    PROTECTED = 0x2
    LOCKED = 0x4


class BranchInfo:
    def __init__(self, name, present=None, protected=None, locked=None):
        self.name = name
        # These are tri-state: None means indeterminate, true or false
        # are definitive.
        self.present = present
        self.protected = protected
        self.locked = locked

    def update(self, other):
        if other.present is not None:
            self.present = other.present
        if other.protected is not None:
            self.protected = other.protected
        if other.locked is not None:
            self.locked = other.locked

    def toDict(self):
        # This doesn't really return a dict, but like other toDict
        # methods, it returns the object that will be encoded into
        # JSON.  It just happens we don't need a full dict for this.
        return [self.flags, self.valid_flags]

    @property
    def flags(self):
        flags = BranchFlag.CLEAR
        if self.present:
            flags |= BranchFlag.PRESENT
        if self.protected:
            flags |= BranchFlag.PROTECTED
        if self.locked:
            flags |= BranchFlag.LOCKED
        return flags

    @property
    def valid_flags(self):
        # If a flag is None, then we don't know it for this branch so
        # we consider it invalid.
        valid_flags = BranchFlag.CLEAR
        if self.present is not None:
            valid_flags |= BranchFlag.PRESENT
        if self.protected is not None:
            valid_flags |= BranchFlag.PROTECTED
        if self.locked is not None:
            valid_flags |= BranchFlag.LOCKED
        return valid_flags

    @classmethod
    def fromDict(cls, name, data):
        o = cls(name)
        flags = BranchFlag(data[0])
        valid_flags = BranchFlag(data[1])

        if BranchFlag.PRESENT in valid_flags:
            o.present = bool(flags & BranchFlag.PRESENT)
        if BranchFlag.PROTECTED in valid_flags:
            o.protected = bool(flags & BranchFlag.PROTECTED)
        if BranchFlag.LOCKED in valid_flags:
            o.locked = bool(flags & BranchFlag.LOCKED)
        return o
