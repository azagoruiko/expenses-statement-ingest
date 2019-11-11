package ua.org.zagoruiko.expenses.spark.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import ua.org.zagoruiko.expenses.category.matcher.Matcher;
import ua.org.zagoruiko.expenses.category.model.Tag;
import ua.org.zagoruiko.expenses.category.model.TransactionMetadata;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

@Component
@PropertySource(value = "classpath:application.properties")
public class ImportPb implements Serializable {
    public static final long serialVersionUID = 0L;

    @Autowired
    @Qualifier("jdbc.url")
    private String jdbcUrl;

    @Autowired
    @Qualifier("jdbc.driver")
    private String jdbcDriver;

    @Autowired
    @Qualifier("jdbc.user")
    private String jdbcUser;

    @Autowired
    @Qualifier("jdbc.password")
    private String jdbcPassword;

    @Value("${jdbc.table}")
    private String jdbcTable = "transactions";

    @Value("${s3.endpoint}")
    private String s3Endpoint;

    @Value("${s3.access.key}")
    private String s3AccessKey;

    @Value("${s3.secret.key}")
    private String s3SecretKey;

    @Autowired
    @Qualifier("pb_matcher")
    private Matcher pbMatcher;

    private static Column cleanNonPrintable(Column col) {
        return regexp_replace(trim(col), "[^\\x00-\\x7F]+", "");
    }

    private static Column cleanFloat(Column col) {
        return regexp_replace(cleanNonPrintable(col),
                ",", ".");
    }

    public static void main(String[] args) throws Exception {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.scan(ImportPb.class.getPackage().getName());
        context.refresh();
        context.getBean(ImportPb.class).run(args);
    }

    public void run(String[] args) throws Exception {
        UDF3<String, String, String, Tuple2<String, String>> detectCategory =
                (provider, category, operation) -> {
                    Map<String, String> record = new HashMap<>();
                    record.put("category", category);
                    record.put("operation", operation);
                    TransactionMetadata data = this.pbMatcher.metadata(record);
                    return new Tuple2<>(data.getCategory(), String.join(",", data.getTags().stream()
                            .map(x -> x.getName()).collect(Collectors.toList())));
                };

        SparkSession spark = SparkSession
                .builder()
                .appName("S3MinioReader")
                .getOrCreate();

        spark.conf().set("spark.executor.userClassPathFirst", "true");

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("category", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("tags", DataTypes.StringType, false));
        DataType schema = DataTypes.createStructType(fields);
        spark.sqlContext().udf().register("category_match", detectCategory, schema);

        SparkContext sparkContext = spark.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sparkContext);

        Configuration conf=jsc.hadoopConfiguration();

        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.endpoint", this.s3Endpoint);
        conf.set("fs.s3a.access.key", this.s3AccessKey);
        conf.set("fs.s3a.secret.key", this.s3SecretKey);

        Properties jdbcProperties = new Properties();
        jdbcProperties.setProperty("driver", this.jdbcDriver);
        jdbcProperties.setProperty("user", this.jdbcUser);
        jdbcProperties.setProperty("password", this.jdbcPassword);
        //create table transactions ( id int primary key auto_increment, transaction_date datetime, category varchar(200), category_id int, amount decimal(13, 2), amount_orig varchar(20), operation varchar(1024), tags varchar(2048));

        Dataset<Row> ds = spark.read()
                .format("csv")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("header", "true")
                .load("s3a://raw.pb.statements/*.csv")
                .withColumn("operation", col("Описание операции"))
                .withColumn("amount_clean", cleanFloat(col("Сумма в валюте карты"))
                        .cast(DataTypes.FloatType))
                .withColumn("amount", col("amount_clean").cast(DataTypes.FloatType))
                .withColumn("raw_category", trim(col("Категория")))
                .withColumn("transaction", callUDF("category_match", lit("pb"), col("raw_category"), col("operation")))
                .select(col("transaction.category").as("category"),
                        col("transaction.tags").as("tags"),
                        trim(col("Категория")).as("raw_category"),
                        col("operation"),
                        col("Дата").as("date"),
                        col("Время").as("time"),
                        cleanNonPrintable(col("Сумма в валюте карты")).as("amount_orig"),
                        col("amount_clean"),
                        col("amount"));
        ds.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("category").option("path", "s3a://buq/pb_normalized.parq")
                .saveAsTable("pb_normalized");

        ds.select(col("category"),
                concat_ws(" ", col("date"), col("time")).as("transaction_time"),
                col("amount"),
                col("amount_orig"),
                col("operation"),
                col("tags"))
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc(this.jdbcUrl, this.jdbcTable, jdbcProperties);

        spark.table("pb_normalized")
                .select("date", "time", "operation", "category", "amount_clean", "amount", "amount_orig")
                .repartition(1)
                .write().mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv("s3a://buq/pb_normalized.csv");
        spark.stop();
    }
}
