FROM 192.168.0.21:9999/docker/spark-nomad-aws-client:test1

WORKDIR /app
COPY target/sparkjob-jar-with-dependencies.jar /app/sparkjob.jar

#COPY statements-ingest-spark.json  /app/statements-ingest-spark.json
COPY bin/run.sh  /app/run.sh

ENV PATH="${PATH}:/opt/spark/bin"
