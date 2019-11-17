#!/usr/bin/env bash

VER=$1

export NOMAD_ADDR=http://192.168.0.21:4646
export CONSUL_HTTP_ADDR=192.168.0.21:8500

export JDBC_URL=$(consul kv get jdbc.url)
export JDBC_DRIVER=$(consul kv get jdbc.driver)
export JDBC_USER=$(consul kv get jdbc.user)
export JDBC_PASSWORD=$(consul kv get jdbc.password)

export S3_ENDPOINT=$(consul kv get expenses/object/storage/fs.s3a.endpoint)
export S3_ACCESS_KEY=$(consul kv get expenses/object/storage/fs.s3a.access.key)
export S3_SECRET_KEY=$(consul kv get expenses/object/storage/fs.s3a.secret.key)

/opt/spark/bin/spark-submit \
  --class ua.org.zagoruiko.expenses.spark.etl.ImportPb \
  --master nomad \
  --deploy-mode client \
  --conf "spark.nomad.dockerImage=127.0.0.1:9999/docker/expenses-statement-ingest:test${VER}" \
  --conf spark.executor.instances=3 \
  --conf spark.nomad.datacenters=home \
  --conf spark.nomad.sparkDistribution=local:/opt/spark \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.driver.userClassPathFirst=true \
  --jars /opt/spark/jars/gson-2.8.5.jar \
  local:/app/sparkjob.jar
