package ua.org.zagoruiko.expenses.spark.etl.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.shaded.com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;
import org.glassfish.jersey.client.ClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import ua.org.zagoruiko.expenses.spark.etl.dto.CurrencyRateDTO;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
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
                        "pb-" + UUID.randomUUID();

        UDF3<Float, String, String, Float> currencyToUAH =
                (rate, currency, date) -> {
                    if (rate == null) {
                        String code = currency;
                        switch(currency) {
                            case "долл":
                                code = "USD";
                                break;
                            case "евро":
                                code = "EUR";
                                break;
                            case "дол":
                                code = "USD";
                                break;
                            case "євро":
                                code = "EUR";
                                break;
                        }
                        String[] dateParts = date.split("\\.");
                        date = dateParts[2] + dateParts[1] + dateParts[0];


                        CloseableHttpClient client = HttpClients.createDefault();
                        HttpGet request = new HttpGet(String.format("https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?date=%s&valcode=%s&json=1", date, code));
                        try {
                            CloseableHttpResponse response = client.execute(request);
                            HttpEntity entity = response.getEntity();
                            if (entity != null) {
                                // return it as a String
                                ObjectMapper om = new ObjectMapper();
                                List<CurrencyRateDTO> data = Arrays.asList(om.readValue(EntityUtils.toString(entity), CurrencyRateDTO[].class));
                                System.out.println("getting rate from !" + date + " " + code + " " + data.get(0).getRate());
                                return data.get(0).getRate();
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    }
                    return rate;
                };


        this.spark.sqlContext().udf().register("parseDate", parseDate, DataTypes.TimestampType);
        this.spark.sqlContext().udf().register("parseAccount", parseAccount, DataTypes.StringType);
        this.spark.sqlContext().udf().register("generateId", generateId, DataTypes.StringType);
        this.spark.sqlContext().udf().register("currencyToUAH", currencyToUAH, DataTypes.FloatType);
    }

    @Override
    public Dataset<Row> load() {
        Dataset<Row> data = spark.read()
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
                .withColumn("id",
                        functions.callUDF("generateId",
                                functions.col("Дата"),
                                functions.col("Время"),
                                functions.col("account"),
                                functions.col("Сумма в валюте карты")))
                .select(functions.col("id"),
                        functions.col("date_time"),
                        functions.col("account"),
                        functions.col("amount").as("amount_currency"),
                        functions.col("operation"),
                        functions.col("Валюта карты").as("currency"),
                        functions.lit("pb"),
                        functions.col("Дата").as("date"),
                        functions.col("Время").as("time"),
                        cleanNonPrintable(functions.col("Сумма в валюте карты")).as("amount_orig"),
                        functions.col("amount_clean"),
                        functions.col("account_orig"),
                        functions.col("amount_clean").as("amount"),
                        functions.trim(functions.col("Категория")).as("raw_category")
                        ).dropDuplicates("id");

        return data.select(
                functions.col("id"),
                functions.col("date_time"),
                functions.col("account"),
                functions.col("amount"),
                functions.col("operation"),
                functions.when(
                        functions.col("currency").equalTo(functions.lit("дол")), functions.lit("USD")
                ).when(
                        functions.col("currency").equalTo(functions.lit("євро")), functions.lit("EUR")
                ).when(
                        functions.col("currency").equalTo(functions.lit("долл")), functions.lit("USD")
                ).when(
                        functions.col("currency").equalTo(functions.lit("евро")), functions.lit("EUR")
                ).when(
                        functions.col("currency").equalTo(functions.lit("грн")), functions.lit("UAH")
                ).otherwise(functions.col("currency")).as("currency"),
                functions.lit("pb").as("source"),
                functions.col("date"),
                functions.col("time"),
                functions.col("amount_orig"),
                functions.col("amount_clean"),
                functions.col("amount_currency"),
                functions.col("account_orig"),
                functions.col("raw_category")
        );
    }
}
