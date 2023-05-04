package ua.org.zagoruiko.expenses.spark.etl.loader;

import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("savedLoader")
public class SavedPbStatementLoader implements StatementLoader {
    private SparkSession spark;

    @Autowired
    public SavedPbStatementLoader(SparkSession spark) {
        this.spark = spark;
    }

    @Override
    public Dataset<Row> load() {
        return this.spark.table("pb_normalized")
                .select("id",
                        "source",
                        "account",
                        "date_time",
                        "amount",
                        "currency",
                        "operation",
                        "date",
                        "time",
                        "raw_category",
                        "amount_clean",
                        "amount_orig",
                        "amount_currency",
                        "account_orig");
    }
}
