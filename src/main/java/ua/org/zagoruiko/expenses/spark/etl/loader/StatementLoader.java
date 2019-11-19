package ua.org.zagoruiko.expenses.spark.etl.loader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface StatementLoader {
    Dataset<Row> load();
}
