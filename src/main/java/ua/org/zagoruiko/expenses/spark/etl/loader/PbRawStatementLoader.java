package ua.org.zagoruiko.expenses.spark.etl.loader;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import ua.org.zagoruiko.expenses.spark.etl.dto.CurrencyRateDTO;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
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

        UDF3<Float, String, String, Float> currencyToUAH =
                (rate, currency, date) -> {
                    if (rate == null) {
                        String code = "USd";
                        switch(currency) {
                            case "долл":
                                code = "USD";
                                break;
                            case "евро":
                                code = "EUR";
                                break;
                        }
                        String[] dateParts = date.split("\\.");
                        date = dateParts[2] + dateParts[1] + dateParts[0];
                        ClientConfig cfg = new ClientConfig();
                        cfg.register(JacksonJsonProvider.class);
                        Client client = ClientBuilder.newBuilder().withConfig(cfg).build();
                        WebTarget target = client.target("https://old.bank.gov.ua/NBUStatService/v1/statdirectory/exchange")
                                .queryParam("date", date)
                                .queryParam("valcode", code)
                                .queryParam("json", "1");
                        Invocation.Builder ib = target.request(MediaType.APPLICATION_JSON);
                        List<CurrencyRateDTO> dto = Arrays.asList(ib.get(CurrencyRateDTO[].class));
                        System.out.println("getting rate from !" + date + " " + code + " " + dto.get(0).getRate());
                        return dto.get(0).getRate();
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
        Dataset<Row> ratesFile = spark.read()
                .format("csv")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("header", "true")
                .load("s3a://rates/*.csv");
        ratesFile.registerTempTable("rates");
        Dataset<Row> rates = this.spark.sql("SELECT\n" +
                "          CASE WHEN `Назва валюти`='Євро (EUR)' THEN 'евро'\n" +
                "               WHEN `Назва валюти`='Долар США (USD)' THEN 'долл'\n" +
                "               WHEN `Назва валюти`='Російський рубль (RUB)' THEN 'руб'\n" +
                "               ELSE 'грн'\n" +
                "          END as currency,\n" +
                "          (`Офіційний курс` / `Кількість одиниць`) as rate,\n" +
                "          `Дата` as dt\n" +
                "          from rates\n" +
                "   ORDER BY dt"
                );
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
                .withColumn("id", functions.callUDF("generateId", functions.col("Дата"), functions.col("Время"), functions.col("account"), functions.col("Сумма в валюте карты")))
                .select(functions.col("id"),
                        functions.col("date_time"),
                        functions.col("account"),
                        functions.col("amount").as("amount_currency"),
                        functions.col("operation"),
                        functions.col("Валюта карты").as("currency1"),
                        functions.lit("pb"),
                        functions.col("Дата").as("date"),
                        functions.col("Время").as("time"),
                        cleanNonPrintable(functions.col("Сумма в валюте карты")).as("amount_orig"),
                        functions.col("amount_clean"),
                        functions.col("account_orig"),
                        functions.trim(functions.col("Категория")).as("raw_category")
                        ).dropDuplicates("id");
        data = data.join( rates, data.col("date").equalTo(rates.col("dt"))
                .and(rates.col("currency").equalTo(data.col("currency1"))), "left");
        data.registerTempTable("joined");
        return spark.sql("SELECT *, IF(currency1 = 'долл' OR currency1 = 'евро', " +
                "(amount_currency * currencyToUAH(CAST(rate as float), currency1, date)), amount_currency) as amount " +
                "FROM joined").select(
                functions.col("id"),
                functions.col("date_time"),
                functions.col("account"),
                functions.col("amount"),
                functions.col("operation"),
                functions.col("currency"),
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
