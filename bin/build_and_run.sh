#!/usr/bin/env bash

VER=$1
BUILD=$2

if [[ ! -z $BUILD ]]; then
    mvn clean install
fi

sudo docker build "--tag=192.168.0.21:9999/docker/expenses-statement-ingest:test${VER}" ./
sudo docker push "192.168.0.21:9999/docker/expenses-statement-ingest:test${VER}"

export NOMAD_ADDR=http://192.168.0.10:4646

cp target/sparkjob-jar-with-dependencies.jar /opt/spark/examples/jars/sparkjob.jar
sudo docker run -it --network host "192.168.0.21:9999/docker/expenses-statement-ingest:test${VER}"  bash /app/run.sh "${VER}"
#bash bin/run.sh "${VER}"
