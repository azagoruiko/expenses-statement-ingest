package ua.org.zagoruiko.expenses.spark.etl.loader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("savedArrayLoader")
public class SavedArrayPbStatementLoader implements StatementLoader {
    private SparkSession spark;

    @Autowired
    public SavedArrayPbStatementLoader(SparkSession spark) {
        this.spark = spark;
    }

    @Override
    public Dataset<Row> load() {
        return this.spark.table("pb_normalized_array")
                .select("id",
                        "account",
                        "date_time",
                        "amount",
                        "operation",
                        "date",
                        "time",
                        "raw_category",
                        "amount_clean",
                        "amount_orig",
                        "account_orig",
                        "tags",
                        "category");
    }
}
