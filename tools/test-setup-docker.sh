#!/bin/bash

# This runs ZooKeeper and databases in docker containers, which are
# required for tests.

# This setup needs to be run as a user that can run docker or podman, or by
# setting $ROOTCMD to a user substitution tool like "sudo" in the calling
# environment.

set -xeu

# Default ROOTCMD to the 'env' command, otherwise variable assignments will be
# interpreted as command when no ROOTCMD is given. The reason for that is
# Bash's simple command expansion.
ROOTCMD=${ROOTCMD:-env}

cd $(dirname $0)
SCRIPT_DIR="$(pwd)"

# Select docker or podman
if command -v docker > /dev/null; then
  DOCKER=docker
elif command -v podman > /dev/null; then
  DOCKER=podman
else
  echo "Please install docker or podman."
  exit 1
fi

# Select docker-compose or podman-compose
if command -v docker-compose > /dev/null; then
  COMPOSE=docker-compose
elif docker compose --help > /dev/null; then
  COMPOSE="docker compose"
elif command -v podman-compose > /dev/null; then
  COMPOSE=podman-compose
else
  echo "Please install docker-compose or podman-compose."
  exit 1
fi


MYSQL="${DOCKER} exec zuul-test-mariadb mysql  -u root -pinsecure_worker"

if [ "${COMPOSE}" == "docker-compose" ]; then
  ${ROOTCMD} docker-compose rm -sf
elif [ "${COMPOSE}" == "docker compose" ]; then
  ${ROOTCMD} docker compose rm -sf
else
  ${ROOTCMD} podman-compose down
fi

CA_DIR=$SCRIPT_DIR/ca

mkdir -p $CA_DIR
$SCRIPT_DIR/zk-ca.sh $CA_DIR zuul-test-zookeeper

# Per-container user namespaces require containers outside a shared pod.
export USER_ID=$(id -u)
if [ "${COMPOSE}" == "podman-compose" ]; then
  ${ROOTCMD} ZUUL_TMPFS_OPTIONS="U,mode=0700" ${COMPOSE} \
    --pod-args='--infra=true --share= --userns=keep-id' up -d
else
  ${ROOTCMD} ZUUL_TMPFS_OPTIONS="uid=${USER_ID}" ${COMPOSE} up -d
fi

echo "Waiting for mysql"
timeout 30 bash -c "until ${ROOTCMD} ${MYSQL} -e 'show databases'; do sleep 0.5; done"
echo

echo "Setting up permissions for zuul tests"
${ROOTCMD} ${MYSQL} -e "CREATE USER 'openstack_citest'@'%' identified by 'openstack_citest';"
${ROOTCMD} ${MYSQL} -e "GRANT ALL PRIVILEGES ON *.* TO 'openstack_citest'@'%' WITH GRANT OPTION;"
${ROOTCMD} ${MYSQL} -u openstack_citest -popenstack_citest -e "SET default_storage_engine=MYISAM; DROP DATABASE IF EXISTS openstack_citest; CREATE DATABASE openstack_citest CHARACTER SET utf8;"

set +x
echo "Finished"

echo "Set this variable to use the in-container pg_dump command:"
echo 'export ZUUL_TEST_PG_DUMP="docker exec -t zuul-test-postgres pg_dump"'
