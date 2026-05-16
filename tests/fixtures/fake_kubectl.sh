#!/bin/bash

# Script arguments look like:
# --kubeconfig=/tmp/tmppm0yyqvv/zuul-test/builds/c21fc1eb7e2c469cb4997d688252dc3c/work/.kube/config --context=zuul-ci-abcdefg:zuul-worker/ -n zuul-ci-abcdefg port-forward pod/fedora-abcdefg 37303:19885

kubeconfig=${1#*=}
if [ ! -f $kubeconfig ]; then
    echo "Unable to find kubeconfig $kubeconfig"
    exit 1
fi

# Get the last argument to the script
arg1=${@: -2:1}
arg2=${@: -1:1}

# Split on the colon
ports1=(${arg1//:/ })
ports2=(${arg2//:/ })

echo "Forwarding from 127.0.0.1:${ports1[0]} -> ${ports1[1]}"
echo "Forwarding from 127.0.0.1:${ports2[0]} -> ${ports2[1]}"

while true; do
    sleep 5
done
