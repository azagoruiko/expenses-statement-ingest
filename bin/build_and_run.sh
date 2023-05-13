#!/usr/bin/env bash
VER=$1
BUILD=$2
if [[ ! -z $BUILD ]]; then
    mvn clean install || exit 1

fi
docker build "--tag=10.8.0.5:5000/expenses-statement-ingest:${VER}" ./
docker push "10.8.0.5:5000/expenses-statement-ingest:${VER}"
export NOMAD_ADDR=http://10.8.0.1:4646
#cp target/sparkjob-jar-with-dependencies.jar /opt/spark/examples/jars/sparkjob.jar
#docker run -it --network host "10.8.0.5:9999/docker/expenses-statement-ingest:${VER}"  bash /app/run.sh "${VER}"
nomad job run statements-ingest.nomad
#bash bin/run.sh "${VER}"
