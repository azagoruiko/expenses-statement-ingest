package ua.org.zagoruiko.expenses.spark.etl.loader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ua.org.zagoruiko.expenses.spark.etl.routines.AlfaBank;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

@Service("alfaRawLoader")
public class AlfaRawStatementLoader implements StatementLoader {
    private SparkSession spark;

    @Autowired
    public AlfaRawStatementLoader(SparkSession spark) {
        this.spark = spark;

        UDF1<String, Timestamp> parseDate =
                (date) ->
                    new Timestamp(new SimpleDateFormat("dd.MM.yyyy")
                            .parse(date).getTime());

        UDF1<String, String> cleanFloat = (amount) -> AlfaBank.cleanAmount(amount);

        UDF4<String, String, String, String, String> generateId =
                (date, account, amount, desc) ->
                        String.format("alfa-%s-%s-%s-%s", date, account, amount, desc.hashCode());


        this.spark.sqlContext().udf().register("parseDateAlfa", parseDate, DataTypes.TimestampType);
        this.spark.sqlContext().udf().register("cleanFloatAlfa", cleanFloat, DataTypes.StringType);
        this.spark.sqlContext().udf().register("generateIdAlfa", generateId, DataTypes.StringType);
    }

    @Override
    public Dataset<Row> load() {
        return spark.read()
                .format("csv")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("header", "true")
                .load("s3a://raw.alfa.statements/*.csv")
                .withColumn("account", functions.col("acc"))
                .withColumn("operation", functions.col("Призначення"))
                .withColumn("amount_clean", functions.callUDF("cleanFloatAlfa", functions.col("Сума"))
                        .cast(DataTypes.FloatType))
                .withColumn("amount", functions.col("amount_clean").cast(DataTypes.FloatType))
                .withColumn("date_time", functions.callUDF("parseDateAlfa", functions.col("Дата операції")))
                .withColumn("id", functions.callUDF("generateIdAlfa", functions.col("Дата операції"), functions.col("account"), functions.col("Сума"), functions.col("Призначення")))

                .select(
                        functions.col("id"),
                        functions.col("date_time"),
                        functions.col("account"),
                        functions.col("amount"),
                        functions.col("operation"),
                        functions.lit("UAH").as("currency"),
                        functions.lit("alfa").as("source"),
                        functions.col("date_time").as("date"),
                        functions.lit("00:00:00").as("time"),
                        functions.col("Сума").as("amount_orig"),
                        functions.col("amount").as("amount_clean"),
                        functions.col("amount").as("amount_currency"),
                        functions.col("account").as("account_orig"),
                        functions.lit("none").as("raw_category")
                        )
                ;//.dropDuplicates("id");
    }
}
