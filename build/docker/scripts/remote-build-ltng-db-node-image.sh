#!/usr/bin/env bash

REMOTE=$1
API_REMOTE=$2
NETRC_LOGIN=$3
NETRC_PASSWORD=$4

docker image build \
    -t pietroski/lightning-db-node \
    -f build/docker/remote/lightning-db-node.Dockerfile \
    --build-arg remote="$REMOTE" \
    --build-arg api_remote="$API_REMOTE" \
    --build-arg netrc_login="$NETRC_LOGIN" \
    --build-arg netrc_password="$NETRC_PASSWORD" \
    . # ../../../
