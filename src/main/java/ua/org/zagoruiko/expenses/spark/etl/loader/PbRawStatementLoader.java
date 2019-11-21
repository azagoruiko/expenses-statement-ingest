package ua.org.zagoruiko.expenses.spark.etl.loader;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service("rawLoader")
public class PbRawStatementLoader implements StatementLoader {
    private SparkSession spark;

    private static Column cleanNonPrintable(Column col) {
        return functions.regexp_replace(functions.trim(col), "[^\\x00-\\x7F]+", "");
    }

    private static Column cleanFloat(Column col) {
        return functions.regexp_replace(cleanNonPrintable(col),
                ",", ".");
    }

    @Autowired
    public PbRawStatementLoader(SparkSession spark) {
        this.spark = spark;

        UDF2<String, String, Timestamp> parseDate =
                (date, time) ->
                    new Timestamp(new SimpleDateFormat("dd.MM.yyyy hh:mm")
                            .parse(date + " " + time).getTime());

        Pattern pattern = Pattern.compile("[\\*]{4}([0-9]{4}?)");
        UDF1<String, String> parseAccount =
                (account) -> {
                    Matcher m = pattern.matcher(account);
                    return m.find() ? m.group(1) : "UNKNOWN";
                };

        UDF4<String, String, String, String, String> generateId =
                (date, time, account, amount) ->
                        String.format("pb-%s-%s-%s-%s", date, time, account, amount);


        this.spark.sqlContext().udf().register("parseDate", parseDate, DataTypes.TimestampType);
        this.spark.sqlContext().udf().register("parseAccount", parseAccount, DataTypes.StringType);
        this.spark.sqlContext().udf().register("generateId", generateId, DataTypes.StringType);
    }

    @Override
    public Dataset<Row> load() {
        return spark.read()
                .format("csv")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("header", "true")
                .load("s3a://raw.pb.statements/*.csv")
                .withColumn("account_orig", functions.col("Карта"))
                .withColumn("operation", functions.col("Описание операции"))
                .withColumn("amount_clean", cleanFloat(functions.col("Сумма в валюте карты"))
                        .cast(DataTypes.FloatType))
                .withColumn("amount", functions.col("amount_clean").cast(DataTypes.FloatType))
                .withColumn("raw_category", functions.trim(functions.col("Категория")))
                .withColumn("date_time", functions.callUDF("parseDate", functions.col("Дата"), functions.col("Время")))
                .withColumn("account", functions.callUDF("parseAccount", functions.col("account_orig")))
                .withColumn("id", functions.callUDF("generateId", functions.col("Дата"), functions.col("Время"), functions.col("account"), functions.col("Сумма в валюте карты")))
                .select(functions.col("id"),
                        functions.col("date_time"),
                        functions.col("account"),
                        functions.col("amount"),
                        functions.col("operation"),
                        functions.lit("pb"),
                        functions.col("Дата").as("date"),
                        functions.col("Время").as("time"),
                        cleanNonPrintable(functions.col("Сумма в валюте карты")).as("amount_orig"),
                        functions.col("amount_clean"),
                        functions.col("account_orig"),
                        functions.trim(functions.col("Категория")).as("raw_category")
                        ).dropDuplicates("id");
    }
}
