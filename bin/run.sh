#!/usr/bin/env bash

VER=$1

export NOMAD_ADDR=http://192.168.0.21:4646
export CONSUL_HTTP_ADDR=192.168.0.21:8500

export JDBC_URL=$(consul kv get jdbc.url)
export JDBC_DRIVER=$(consul kv get jdbc.driver)
export JDBC_USER=$(consul kv get jdbc.user)
export JDBC_PASSWORD=$(consul kv get jdbc.password)

export POSTGRES_METASTORE_JDBC_URL=$(consul kv get hive.postgres.metastore.jdbc.url)
export POSTGRES_JDBC_URL=$(consul kv get postgres.jdbc.url)
export POSTGRES_JDBC_DRIVER=$(consul kv get postgres.jdbc.driver)
export POSTGRES_JDBC_USER=$(consul kv get postgres.jdbc.user)
export POSTGRES_JDBC_PASSWORD=$(consul kv get postgres.jdbc.password)

export S3_ENDPOINT=$(consul kv get expenses/object/storage/fs.s3a.endpoint)
export S3_ACCESS_KEY=$(consul kv get expenses/object/storage/fs.s3a.access.key)
export S3_SECRET_KEY=$(consul kv get expenses/object/storage/fs.s3a.secret.key)

export SERVICE_MATCHER_BASE_URL=$(consul kv get expenses/service/matcher/base_url)
export SERVICE_GOALS_BASE_URL=$(consul kv get telegram/bot/accounter/goals.base.url)
export SERVICE_SPREADSHEETS_BASE_URL=$(consul kv get expenses/google/base_url)

/opt/spark/bin/spark-submit \
  --class ua.org.zagoruiko.expenses.spark.etl.ImportPb \
  --master nomad \
  --deploy-mode client \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.datanucleus.autoCreateSchema=true \
  --conf spark.hadoop.datanucleus.autoCreateTables=true \
  --conf spark.hadoop.javax.jdo.option.ConnectionURL=${POSTGRES_METASTORE_JDBC_URL} \
  --conf spark.hadoop.javax.jdo.option.ConnectionDriverName=${POSTGRES_JDBC_DRIVER} \
  --conf spark.hadoop.javax.jdo.option.ConnectionUserName=${POSTGRES_JDBC_USER} \
  --conf spark.hadoop.javax.jdo.option.ConnectionPassword=${POSTGRES_JDBC_PASSWORD} \
  --conf "spark.nomad.dockerImage=127.0.0.1:9999/docker/expenses-statement-ingest:${VER}" \
  --conf spark.executor.instances=3 \
  --conf spark.cores.max=6 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.nomad.datacenters=home \
  --conf spark.nomad.sparkDistribution=local:/opt/spark \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.nomad.job.template=/app/statements-ingest-spark.json \
  --jars local:/opt/spark/jars/gson-2.8.5.jar \
  local:/app/sparkjob.jar
