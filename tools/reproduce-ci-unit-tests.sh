#!/bin/bash
# Reproduce the 14.3.0 check-pipeline unit-test job well enough to run
# tests.unit.test_launcher the way Zuul CI does.
#
# What CI runs (from .zuul.yaml at 14.3.0, parent jobs in zuul/zuul-jobs):
#
#   job: zuul-nox
#   parent: nox -> unittests
#   nodeset: ubuntu-jammy
#   pre-run: playbooks/zuul-nox/pre.yaml
#     - ensure-zookeeper (zookeeper_use_tls: true, version "latest" = 3.9.3)
#     - restart ZooKeeper under eatmydata
#     - chown the current cgroup (ZUUL_TEST_CGROUPS)
#   unittests pre-run:
#     - bindep (profile test) installs tools/test-setup.sh dependencies
#     - tools/test-setup.sh with
#         ZUUL_TEST_ROOT=/tmp/zuul-test   (tmpfs, 64M)
#         ZUUL_TEST_CGROUPS=1
#   command (nox role, no session override):
#     nox --force-python 3.11
#   noxfile default sessions include the unit-test session `tests`:
#     pip install -r requirements.txt -r test-requirements.txt
#     pip install -e .
#     tools/yarn-build.sh          # dashboard; launcher tests do not use it
#     zuul-manage-ansible -v
#     stestr run --slowest --concurrency=$(( cpu * 70 / 100 ))
#   environment passed into nox:
#     ZUUL_ZK_CA=/opt/zookeeper/ca/certs/cacert.pem
#     ZUUL_ZK_CERT=/opt/zookeeper/ca/certs/client.pem
#     ZUUL_ZK_KEY=/opt/zookeeper/ca/keys/clientkey.pem
#     ZUUL_TEST_ROOT=/tmp/zuul-test
#     CI=1
#   noxfile also sets OS_LOG_CAPTURE, OS_STDERR_CAPTURE, OS_STDOUT_CAPTURE,
#   OS_TEST_TIMEOUT=360, PYTHONTRACEMALLOC=0, and a PYTHONWARNINGS list.
#
# ZooKeeper TLS matches roles/ensure-zookeeper/tasks/setup_tls.yaml:
#   NettyServerCnxnFactory, secureClientPort=2281, CN=localhost server cert,
#   client cert, CA from tools/zk-ca.sh. This host has no jammy python3.11
#   package, so CPython 3.11 comes from uv. ZooKeeper's default max session
#   timeout is tickTime*20 (40s); Zuul asks for 120s, so zoo.cfg also sets
#   maxSessionTimeout=120000.
#
# Usage:
#   tools/reproduce-ci-unit-tests.sh setup
#   tools/reproduce-ci-unit-tests.sh python-launcher
#   ZUUL_LAUNCHER_IMPL=rust tools/reproduce-ci-unit-tests.sh rust-launcher

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV="${ZUUL_CI_VENV:-$HOME/zuul-nox-311}"
PY="${ZUUL_CI_PYTHON:-3.11}"
export PATH="${HOME}/.local/bin:${PATH}"

ci_env() {
    export CI=1
    export ZUUL_ZK_CA=/opt/zookeeper/ca/certs/cacert.pem
    export ZUUL_ZK_CERT=/opt/zookeeper/ca/certs/client.pem
    export ZUUL_ZK_KEY=/opt/zookeeper/ca/keys/clientkey.pem
    export ZUUL_TEST_ROOT="${ZUUL_TEST_ROOT:-/tmp/zuul-test}"
    export OS_LOG_CAPTURE=1
    export OS_STDERR_CAPTURE=1
    export OS_STDOUT_CAPTURE=1
    export OS_TEST_TIMEOUT=360
    export PYTHONTRACEMALLOC=0
    export PYTHONWARNINGS="always::DeprecationWarning:zuul.driver.sql.sqlconnection,always::DeprecationWarning:tests.base,always::DeprecationWarning:tests.unit.test_database,always::DeprecationWarning:zuul.driver.sql.alembic.env,always::DeprecationWarning:zuul.driver.sql.alembic.script"
}

setup_python() {
    if ! command -v uv >/dev/null 2>&1; then
        /usr/bin/python3 -m pip install --user uv
    fi
    uv python install "$PY"
    uv venv "$VENV" --python "$PY"
    # shellcheck disable=SC1091
    source "$VENV/bin/activate"
    uv pip install -r "$ROOT/requirements.txt" -r "$ROOT/test-requirements.txt"
    uv pip install -e "$ROOT"
    uv pip install maturin
    zuul-manage-ansible -v
    # Same interpreter the nox session uses, so the opt-in Rust launcher
    # loads in this environment.
    ( cd "$ROOT/rust/zuul-launcher-py" && maturin develop )
}

setup_zookeeper_paths() {
    sudo mkdir -p /opt/zookeeper
    if [[ ! -e /opt/zookeeper/ca ]]; then
        sudo ln -sfn "$ROOT/tools/ca" /opt/zookeeper/ca
    fi
    if [[ ! -f /opt/zookeeper/ca/certs/cacert.pem ]]; then
        bash "$ROOT/tools/zk-ca.sh" "$ROOT/tools/ca" localhost
    fi
}

setup_test_root() {
    ci_env
    sudo mkdir -p "$ZUUL_TEST_ROOT"
    if ! findmnt -n "$ZUUL_TEST_ROOT" >/dev/null 2>&1; then
        sudo mount -t tmpfs -o noatime,nodev,nosuid,size=64M none "$ZUUL_TEST_ROOT" \
            || sudo mkdir -p "$ZUUL_TEST_ROOT"
    fi
    sudo chmod 1777 "$ZUUL_TEST_ROOT"
}

setup_packages() {
    # bindep test profile pieces this image does not already have.
    # zstd is required by the launcher image-upload jobs.
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y zstd
}

concurrency() {
    local n procs
    n="$(nproc)"
    procs=$(( n * 70 / 100 ))
    if [[ "$procs" -lt 1 ]]; then
        procs=1
    fi
    echo "$procs"
}

run_stestr() {
    ci_env
    # shellcheck disable=SC1091
    source "$VENV/bin/activate"
    cd "$ROOT"
    rm -rf .stestr
    local procs
    procs="$(concurrency)"
    echo "stestr concurrency=${procs} python=$(python -V) impl=${ZUUL_LAUNCHER_IMPL:-python}"
    PYTHONUNBUFFERED=1 stestr run --slowest "--concurrency=${procs}" \
        tests.unit.test_launcher
}

cmd="${1:-setup}"
case "$cmd" in
    setup)
        setup_packages
        setup_zookeeper_paths
        setup_test_root
        setup_python
        ;;
    python-launcher)
        unset ZUUL_LAUNCHER_IMPL
        run_stestr
        ;;
    rust-launcher)
        export ZUUL_LAUNCHER_IMPL=rust
        run_stestr
        ;;
    *)
        echo "usage: $0 {setup|python-launcher|rust-launcher}" >&2
        exit 2
        ;;
esac
