package ua.org.zagoruiko.expenses.spark.etl.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

@Component("taggedWriter")
public class TaggedStatementWriter implements StatementWriter {
    @Override
    public void write(Dataset<Row> dataset) {

    }
}
