package ua.org.zagoruiko.expenses.spark.etl.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.springframework.stereotype.Component;

@Component("csvWriter")
public class CsvStatementWriter implements StatementWriter {
    @Override
    public void write(Dataset<Row> dataset) {
        dataset.repartition(1)
                .write().mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv("s3a://buq/pb_normalized.csv");
    }
}
