#!/usr/bin/env bash
VER=$1
BUILD=$2
if [[ ! -z $BUILD ]]; then
    mvn clean install || exit 1

fi
docker build "--tag=10.8.0.5:5000/expenses-statement-ingest:${VER}" ./
docker push "10.8.0.5:5000/expenses-statement-ingest:${VER}"
export NOMAD_ADDR=http://10.8.0.1:4646
nomad job run statements-ingest.nomad
