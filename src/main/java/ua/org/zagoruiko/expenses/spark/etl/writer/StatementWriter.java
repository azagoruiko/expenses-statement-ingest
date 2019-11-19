package ua.org.zagoruiko.expenses.spark.etl.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface StatementWriter {
    void write(Dataset<Row> dataset);
}
