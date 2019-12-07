package ua.org.zagoruiko.expenses.spark.etl.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ua.org.zagoruiko.expenses.category.matcher.Matcher;
import ua.org.zagoruiko.expenses.category.model.Tag;
import ua.org.zagoruiko.expenses.matcherservice.dto.MatcherSetDTO;
import ua.org.zagoruiko.expenses.matcherservice.matcher.MatcherFatory;
import ua.org.zagoruiko.expenses.spark.etl.matcher.MatcherClient;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

@Component("taggedArrayWriter")
public class TaggedArrayStatementWriter implements StatementWriter {
    @Autowired
    private MatcherClient matcherClient;

    @Override
    public void write(Dataset<Row> dataset) {
        MatcherSetDTO matchs = this.matcherClient.getMatchers();

        Map<String, Matcher<String>> matchers = MatcherFatory.createMatchersMapFromDTO(matchs.getTagsMatcher());

        UDF2<String, String, String[]> detectCategory =
                (operation, source) -> {
                    Set<Tag> match = matchers.get(source).match(operation);
                    List<String> tags = match.stream().map(t -> t.getName()).collect(Collectors.toList());
                    return tags.toArray(new String[tags.size()]);
                };

        dataset.sparkSession().sqlContext().udf().register("category_match_array", detectCategory, DataTypes.createArrayType(DataTypes.StringType));

        dataset.withColumn("tags", functions.callUDF("category_match_array", col("operation"), col("source")))
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("account").option("path", "s3a://buq/pb_normalized.tagged.array.parq")
                .saveAsTable("pb_normalized_array");;
    }
}
