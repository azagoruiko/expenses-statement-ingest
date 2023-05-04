package ua.org.zagoruiko.expenses.spark.etl.config;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

@Component
@Configuration
public class SparkConfig {
    private S3Config s3Config;

    @Autowired
    public SparkConfig(S3Config s3Config) {
        this.s3Config = s3Config;
    }

    @Bean
    public SparkSession sparkSession() {
        SparkSession spark = SparkSession
                .builder()
                .appName("PB_Statement_CSV_Ingest")
                .getOrCreate();

        SparkContext sparkContext = spark.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sparkContext);

        org.apache.hadoop.conf.Configuration conf=jsc.hadoopConfiguration();

        System.out.println(String.format("Setting %s (endpont) to %s", "fs.s3a.endpoint", this.s3Config.getS3Endpoint()));

        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.endpoint", this.s3Config.getS3Endpoint());
        conf.set("fs.s3a.access.key", this.s3Config.getS3AccessKey());
        conf.set("fs.s3a.secret.key", this.s3Config.getS3SecretKey());

        return spark;
    }
}
