package ua.org.zagoruiko.expenses.spark.etl.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ua.org.zagoruiko.expenses.category.matcher.Matcher;
import ua.org.zagoruiko.expenses.category.model.Tag;
import ua.org.zagoruiko.expenses.matcherservice.dto.CategoryTagDTO;
import ua.org.zagoruiko.expenses.matcherservice.dto.MatcherSetDTO;
import ua.org.zagoruiko.expenses.matcherservice.matcher.MatcherFatory;
import ua.org.zagoruiko.expenses.spark.etl.matcher.MatcherClient;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

@Component("taggedArrayWriter")
public class TaggedArrayStatementWriter implements StatementWriter {
    @Autowired
    private MatcherClient matcherClient;

    @Override
    public void write(Dataset<Row> dataset) {
        MatcherSetDTO matchs = this.matcherClient.getMatchers();
        List<CategoryTagDTO> catTags = this.matcherClient.getCategoryTags();

        Map<String, Matcher<String>> matchers = MatcherFatory.createMatchersMapFromDTO(matchs.getTagsMatcher());

        UDF2<String, String, String[]> detectCategory =
                (operation, source) -> {
                    if (!matchers.containsKey(source)) {
                        return new String[] {"OTHER"};
                    }
                    Set<Tag> match = matchers.get(source).match(operation);
                    List<String> tags = match.stream().map(t -> t.getName()).collect(Collectors.toList());
                    return tags.toArray(new String[tags.size()]);
                };

        UDF1<String, String> detectMainCategory =
                (tagsStr) -> {
                    String [] tags = tagsStr.split(",");
                    List<CategoryTagDTO> allMatches =
                            catTags.stream().filter(ct -> Arrays.stream(tags)
                                    .anyMatch(t -> t.equals(ct.getTag()))).collect(Collectors.toList());

                    long count = allMatches.size();
                    if (count == 0) {
                        return "OTHER";
                    } else {
                        if (count > 1) {
                            allMatches = allMatches.stream().filter(ct -> !ct.isExclusive()).collect(Collectors.toList());
                        }
                        return allMatches.stream().findFirst().get().getTag();
                    }

                };

        dataset.sparkSession().sqlContext().udf().register("category_match_array", detectCategory, DataTypes.createArrayType(DataTypes.StringType));
        dataset.sparkSession().sqlContext().udf().register("main_category2", detectMainCategory, DataTypes.StringType);

        dataset
                .withColumn("tags", functions.callUDF("category_match_array", col("operation"), col("source")))
                //.withColumn("category", functions.lit("NONE"))
                .withColumn("category", functions.callUDF("main_category2", functions.concat_ws(",", col("tags"))))
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("account").option("path", "s3a://normalized/pb_normalized.tagged.array.parq")
                .saveAsTable("pb_normalized_array");
    }
}
