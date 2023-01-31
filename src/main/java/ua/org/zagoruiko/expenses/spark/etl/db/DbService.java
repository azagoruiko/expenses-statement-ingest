package ua.org.zagoruiko.expenses.spark.etl.db;

import java.sql.SQLException;

public interface DbService {
    void refreshTransactionsMaterializedView() throws SQLException;
}
