#!/usr/bin/env bash
VER=$1
BUILD=$2
if [[ ! -z $BUILD ]]; then
    mvn clean install
fi
docker build "--tag=192.168.0.21:9999/docker/expenses-statement-ingest:${VER}" ./
docker push "192.168.0.21:9999/docker/expenses-statement-ingest:${VER}"
export NOMAD_ADDR=http://192.168.0.21:4646
#cp target/sparkjob-jar-with-dependencies.jar /opt/spark/examples/jars/sparkjob.jar
#docker run -it --network host "192.168.0.21:9999/docker/expenses-statement-ingest:${VER}"  bash /app/run.sh "${VER}"
nomad job run statements-ingest.nomad
#bash bin/run.sh "${VER}"
