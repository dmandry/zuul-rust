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

import multiprocessing
import os

import nox


nox.options.error_on_external_run = True
nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = ["tests-3", "linters"]


def set_env(session, var, default):
    session.env[var] = os.environ.get(var, default)


def set_standard_env_vars(session):
    set_env(session, 'OS_LOG_CAPTURE', '1')
    set_env(session, 'OS_STDERR_CAPTURE', '1')
    set_env(session, 'OS_STDOUT_CAPTURE', '1')
    set_env(session, 'OS_TEST_TIMEOUT', '360')
    session.env['PYTHONWARNINGS'] = ','.join([
        'always::DeprecationWarning:zuul.driver.sql.sqlconnection',
        'always::DeprecationWarning:tests.base',
        'always::DeprecationWarning:tests.unit.test_database',
        'always::DeprecationWarning:zuul.driver.sql.alembic.env',
        'always::DeprecationWarning:zuul.driver.sql.alembic.script',
    ])
    # Set PYTHONTRACEMALLOC to a value greater than 0 in the calling env
    # to get tracebacks of that depth for ResourceWarnings. Disabled by
    # default as this consumes more resources and is slow.
    set_env(session, 'PYTHONTRACEMALLOC', '0')


@nox.session(python='3')
def bindep(session):
    set_standard_env_vars(session)
    session.install('bindep')
    session.run('bindep', 'test')


@nox.session(python='3')
def cover(session):
    set_standard_env_vars(session)
    session.env['PYTHON'] = 'coverage run --source zuul --parallel-mode'
    session.install('-r', 'requirements.txt',
                    '-r', 'test-requirements.txt')
    session.install('-e', '.')
    session.run('stestr', 'run')
    session.run('coverage', 'combine')
    session.run('coverage', 'html', '-d', 'cover')
    session.run('coverage', 'xml', '-o', 'cover/coverage.xml')


@nox.session(python='3')
def docs(session):
    set_standard_env_vars(session)
    # Setuptools 82.0.0 removed pkg_resources entirely. sphinxcontrib-openapi
    # depends on pkg_resources and docs builds fail without this pin. We can
    # remove the setuptools pin once sphinxcontrib-openapi drops the use of
    # pkg_resources.
    session.install('setuptools<82.0.0')
    session.install('-r', 'doc/requirements.txt',
                    '-r', 'test-requirements.txt')
    session.install('-e', '.')
    session.run('sphinx-build', '-E', '-W', '-d', 'doc/build/doctrees',
                '-b', 'html', 'doc/source/', 'doc/build/html')


@nox.session(python='3')
def linters(session):
    set_standard_env_vars(session)
    session.install('flake8', 'openapi-spec-validator')
    session.run('flake8')
    session.run('openapi-spec-validator', 'web/public/openapi.yaml')


@nox.session(python='3')
def tests(session):
    set_standard_env_vars(session)
    session.install('-r', 'requirements.txt',
                    '-r', 'test-requirements.txt')
    session.install('-e', '.')
    session.run_always('tools/yarn-build.sh', external=True)
    session.run_always('zuul-manage-ansible', '-v')
    procs = max(int(multiprocessing.cpu_count() * 0.5), 1)
    session.run('stestr', 'run', '--slowest', f'--concurrency={procs}',
                *session.posargs)


@nox.session(python='3')
def upgrade(session):
    set_standard_env_vars(session)
    session.install('-r', 'requirements.txt',
                    '-r', 'test-requirements.txt')
    session.install('-e', '.')
    session.run_always('zuul-manage-ansible', '-v')
    procs = max(int(multiprocessing.cpu_count() * 0.75), 1)
    session.run('stestr', 'run', '--test-path', './tests/upgrade',
                '--slowest', f'--concurrency={procs}',
                *session.posargs)
    # Output the test log to stdout so we have a copy of even the
    # successful output.  We capture and output instead of just
    # streaming it so that it's not interleaved.
    session.run('stestr', 'last', '--all-attachments')


@nox.session(python='3')
def remote(session):
    set_standard_env_vars(session)
    session.install('-r', 'requirements.txt',
                    '-r', 'test-requirements.txt')
    session.install('-e', '.')
    session.run_always('zuul-manage-ansible', '-v')
    session.run('stestr', 'run', '--test-path', './tests/remote',
                *session.posargs)


@nox.session(python='3')
def venv(session):
    set_standard_env_vars(session)
    session.install('-r', 'requirements.txt',
                    '-r', 'test-requirements.txt')
    session.install('-e', '.')
    session.run(*session.posargs)


@nox.session(python='3')
def zuul_client(session):
    set_standard_env_vars(session)
    session.install('zuul-client',
                    '-r', 'test-requirements.txt',
                    '-r', 'requirements.txt')
    session.install('-e', '.')
    session.run_always('zuul-manage-ansible', '-v')
    session.run(
        'stestr', 'run', '--concurrency=1',
        '--test-path', './tests/zuul_client')
