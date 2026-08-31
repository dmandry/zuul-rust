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
import contextlib
import pathlib
from zuul.executor.exceptions import PathTraversalError


@contextlib.contextmanager
def _safe_open(file, *args, **kwargs):
    if (resolved := str(pathlib.Path(file).resolve())) != file:
        raise PathTraversalError(f"Symlink detected {file} -> {resolved}")

    with open(file, *args, **kwargs) as f:
        yield f


def _safe_exists(path):
    if (resolved := str(pathlib.Path(path).resolve())) != path:
        raise PathTraversalError(f"Symlink detected {path} -> {resolved}")
    return os.path.exists(resolved)


def _safe_unlink(path):
    if (resolved := str(pathlib.Path(path).resolve())) != path:
        raise PathTraversalError(f"Symlink detected {path} -> {resolved}")
    return os.unlink(resolved)
