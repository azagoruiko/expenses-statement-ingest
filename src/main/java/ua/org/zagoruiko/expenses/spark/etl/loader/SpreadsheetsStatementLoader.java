package ua.org.zagoruiko.expenses.spark.etl.loader;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ua.org.zagoruiko.expenses.spark.etl.matcher.SpreadsheetsClient;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;


@Service("spreadsheetsRawLoader")
public class SpreadsheetsStatementLoader implements StatementLoader {
    private SparkSession spark;

    @Autowired
    public SpreadsheetsStatementLoader(SparkSession spark) {
        this.spark = spark;


        UDF1<String, Timestamp> parseDate =
                (date) ->
                        new Timestamp(new SimpleDateFormat("yyyy-MM-dd")
                                .parse(date).getTime());

        this.spark.sqlContext().udf().register("parseDateSpreadsheets", parseDate, DataTypes.TimestampType);
    }

    @Override
    public Dataset<Row> load() {

        return spark.read()
                .format("csv")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("header", "true")
                .load("s3a://spreadsheet.archive/archive.csv")
                .withColumn("date_time", functions.callUDF("parseDateSpreadsheets", functions.col("date")))
                .withColumn("amount_clean", functions.col("amount"))
                .withColumn("account", functions.lit("spreadsheets"))
                .select(
                        functions.col("id"),
                        functions.col("date_time"),
                        functions.col("account"),
                        functions.col("amount").cast(DataTypes.FloatType),
                        functions.col("operation"),
                        functions.lit("UAH").as("currency"),
                        functions.lit("spreadsheets").as("source"),
                        functions.col("date_time").as("date"),
                        functions.lit("00:00:00").as("time"),
                        functions.col("amount").as("amount_orig"),
                        functions.col("amount").as("amount_clean"),
                        functions.col("amount").as("amount_currency"),
                        functions.col("account").as("account_orig"),
                        functions.col("category").as("raw_category")
                );
    }
}
