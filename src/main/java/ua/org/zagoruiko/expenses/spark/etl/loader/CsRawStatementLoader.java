package ua.org.zagoruiko.expenses.spark.etl.loader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.UUID;

@Service("csRawLoader")
public class CsRawStatementLoader implements StatementLoader {
    private SparkSession spark;

    @Autowired
    public CsRawStatementLoader(SparkSession spark) {
        this.spark = spark;

        UDF1<String, Timestamp> parseDate =
                (date) ->
                    date  == null ? new Timestamp(0l) : new Timestamp(new SimpleDateFormat("dd.MM.yyyy")
                            .parse(date).getTime());

        UDF1<String, String> cleanFloat = (amount) -> amount.replace(",", "").trim();

        UDF3<String, String, String, String> generateId =
                (date, account, desc) ->
                        "cs-" + UUID.randomUUID();

        this.spark.sqlContext().udf().register("parseDateCs", parseDate, DataTypes.TimestampType);
        this.spark.sqlContext().udf().register("cleanFloatCs", cleanFloat, DataTypes.StringType);
        this.spark.sqlContext().udf().register("generateIdCs", generateId, DataTypes.StringType);
    }

    @Override
    public Dataset<Row> load() {
        //"Own Account Number",
        // "Processing Date",
        // "Partner Name",
        // "Partner IBAN",
        // "BIC/SWIFT",
        // "Partner Account Number",
        // "Bank code",
        // "Amount",
        // "Currency",
        // "Variable Symbol",
        // "Transaction Type",
        // "Message for me",
        // "Message for recipient",
        // "Note","Category",
        // "Card Location"
        return spark.read()
                .format("csv")
                .option("quote", "\"")
                //.option("escape", "\"")
                .option("header", "true")
                .option("encoding", "UTF-8")
                .load("s3a://raw.cs.statements/*.csv")
                .withColumn("account", functions.col("Own Account Number"))
                .withColumn("operation", functions.concat(
                        functions.lit("Partner Name: "),           functions.coalesce(functions.col("Partner Name"), functions.lit("null!")),          functions.lit("; "),
                        functions.lit("Partner Account Number: "), functions.coalesce(functions.col("Partner Account Number"), functions.lit("null!")),functions.lit("; "),
                        functions.lit("Transaction Type: "),       functions.coalesce(functions.col("Transaction Type"), functions.lit("null!")),      functions.lit("; "),
                        functions.lit("Message for me: "),         functions.coalesce(functions.col("Message for me"), functions.lit("null!")),        functions.lit("; "),
                        functions.lit("Message for recipient: "),  functions.coalesce(functions.col("Message for recipient"), functions.lit("null!")), functions.lit("; "),
                        functions.lit("Category: "),               functions.coalesce(functions.col("Category"), functions.lit("null!")),              functions.lit("; "),
                        functions.lit("Card Location: "),          functions.coalesce(functions.col("Card Location"), functions.lit("null!")),         functions.lit("; ")
                ))

                .withColumn("currency", functions.col("Currency"))
                .withColumn("Category", functions.col("Category"))
                .withColumn("amount_clean", functions.callUDF("cleanFloatCs", functions.col("Amount"))
                        .cast(DataTypes.FloatType))
                .withColumn("amount", functions.col("amount_clean").cast(DataTypes.FloatType))
                .withColumn("date_time", functions.callUDF("parseDateCs", functions.col("Processing Date")))
                .withColumn("id", functions.callUDF("generateIdCs", functions.col("Processing Date"), functions.col("Own Account Number"), functions.col("Partner Name")))
                //.withColumn("id", functions.("cs-" + UUID.randomUUID().toString()))

                .select(
                        functions.col("id"),
                        functions.col("date_time"),
                        functions.col("account"),
                        functions.col("amount"),
                        functions.col("operation"),
                        functions.col("currency"),
                        functions.lit("cs").as("source"),
                        functions.col("date_time").as("date"),
                        functions.lit("00:00:00").as("time"),
                        functions.col("amount").as("amount_orig"),
                        functions.col("amount").as("amount_clean"),
                        functions.col("amount").as("amount_currency"),
                        functions.col("account").as("account_orig"),
                        functions.lit("Category").as("raw_category")
                        )
                ;//.dropDuplicates("id");
    }
}
