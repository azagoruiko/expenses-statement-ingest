#!/usr/bin/env bash
VER=$1
BUILD=$2
if [[ ! -z $BUILD ]]; then
    mvn clean install || exit 1

fi
docker build "--tag=docker-registry.service.consul:5001/expenses-statement-ingest:${VER}" ./
docker push "docker-registry.service.consul:5001/expenses-statement-ingest:${VER}"
export NOMAD_ADDR=http://10.7.0.1:4646
/usr/bin/nomad job run statements-ingest.nomad
