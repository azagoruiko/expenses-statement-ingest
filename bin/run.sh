#!/usr/bin/env bash

VER=$1
echo "RUNNING MASTER $SPARK_MASTER"
/opt/spark/bin/spark-submit \
  --class ua.org.zagoruiko.expenses.spark.etl.ImportPb \
  --master spark://$SPARK_MASTER \
  --deploy-mode client \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.datanucleus.autoCreateSchema=true \
  --conf spark.hadoop.datanucleus.autoCreateTables=true \
  --conf spark.hadoop.javax.jdo.option.ConnectionURL=${POSTGRES_METASTORE_JDBC_URL} \
  --conf spark.hadoop.javax.jdo.option.ConnectionDriverName=${POSTGRES_JDBC_DRIVER} \
  --conf spark.hadoop.javax.jdo.option.ConnectionUserName=${POSTGRES_JDBC_USER} \
  --conf spark.hadoop.javax.jdo.option.ConnectionPassword=${POSTGRES_JDBC_PASSWORD} \
  --conf spark.executor.instances=2 \
  --conf spark.cores.max=6 \
  https://objectstorage.us-ashburn-1.oraclecloud.com/p/MGjOh9KK3Lotr7uUKKC8VPikPvbjGzWHxNIDrwfjy7hpB4xeYXCNMMO6E_uGz_T8/n/idnbbkjxiqcc/b/jars/o/expenses.jar
