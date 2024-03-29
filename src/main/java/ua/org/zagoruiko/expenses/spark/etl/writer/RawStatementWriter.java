package ua.org.zagoruiko.expenses.spark.etl.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.springframework.stereotype.Component;

@Component("rawWriter")
public class RawStatementWriter implements StatementWriter {
    @Override
    public void write(Dataset<Row> dataset) {
        dataset.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("source")
                .option("path", "s3a://normalized/pb_normalized.parq")
                .saveAsTable("pb_normalized");
    }
}
