package ua.org.zagoruiko.expenses.spark.etl.loader;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.trim;

@Service("taggedLoader")
public class PbTaggedStatementLoader implements StatementLoader {
    private SparkSession spark;

    private static Column cleanNonPrintable(Column col) {
        return regexp_replace(trim(col), "[^\\x00-\\x7F]+", "");
    }

    private static Column cleanFloat(Column col) {
        return regexp_replace(cleanNonPrintable(col),
                ",", ".");
    }

    @Autowired
    public PbTaggedStatementLoader(SparkSession spark) {
        this.spark = spark;

        UDF3<String, String, String, Tuple2<String, String>> detectCategory =
                (provider, category, operation) -> {
                    ClientConfig cfg = new ClientConfig();
                    cfg.register(JacksonJsonProvider.class);
                    Client client = ClientBuilder.newBuilder().withConfig(cfg).build();
                    WebTarget target = client.target("http://192.168.0.101:8080/matchers/match")
                            .queryParam("text", operation);
                    Invocation.Builder ib = target.request(MediaType.APPLICATION_JSON);
                    List<String> data = Arrays.asList(ib.get( String[].class));
                    return new Tuple2<>("UNKNOWN", String.join(",", data));
                };

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("category", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("tags", DataTypes.StringType, false));
        DataType schema = DataTypes.createStructType(fields);
        this.spark.sqlContext().udf().register("category_match", detectCategory, schema);
    }

    @Override
    public Dataset<Row> load() {
        return spark.read()
                .format("csv")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("header", "true")
                .load("s3a://raw.pb.statements/*.csv")
                .withColumn("account", col("Карта"))
                .withColumn("operation", col("Описание операции"))
                .withColumn("amount_clean", cleanFloat(col("Сумма в валюте карты"))
                        .cast(DataTypes.FloatType))
                .withColumn("amount", col("amount_clean").cast(DataTypes.FloatType))
                .withColumn("raw_category", trim(col("Категория")))
                .withColumn("transaction", functions.callUDF("category_match", lit("pb"), col("raw_category"), col("operation")))
                .select(col("transaction.category").as("category"),
                        col("transaction.tags").as("tags"),
                        trim(col("Категория")).as("raw_category"),
                        col("operation"),
                        col("Дата").as("date"),
                        col("Время").as("time"),
                        cleanNonPrintable(col("Сумма в валюте карты")).as("amount_orig"),
                        col("amount_clean"),
                        col("amount"));
    }
}
