package ua.org.zagoruiko.expenses.spark.etl.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import ua.org.zagoruiko.expenses.category.matcher.Matcher;
import ua.org.zagoruiko.expenses.matcherservice.dto.MatcherSetDTO;
import ua.org.zagoruiko.expenses.matcherservice.matcher.MatcherFatory;
import ua.org.zagoruiko.expenses.spark.etl.matcher.MatcherClient;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

@Component("taggedCsvWriter")
public class TaggedCsvStatementWriter implements StatementWriter {
    @Autowired
    private MatcherClient matcherClient;

    @Override
    public void write(Dataset<Row> dataset) {
        MatcherSetDTO matchers = this.matcherClient.getMatchers("pb");

        Matcher<String> matcher = MatcherFatory.createMatcherFromDTO(matchers.getTagsMatcher(), "pb");

        UDF1<String, String> detectCategory =
                (operation) -> String.join(",",
                        matcher.match(operation).stream().map(t -> t.getName()).collect(Collectors.toList()));

        dataset.sparkSession().sqlContext().udf().register("category_match", detectCategory, DataTypes.StringType);

        dataset.repartition(1)
                .withColumn("tags", functions.callUDF("category_match", col("operation")))
                .write().mode(SaveMode.Overwrite)
                .partitionBy("source")
                .option("header", "true")
                .csv("s3a://normalized/pb_normalized.tagged.csv");
    }
}
