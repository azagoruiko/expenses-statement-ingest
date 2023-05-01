package ua.org.zagoruiko.expenses.spark.etl.db;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.SQLException;

@Service
public class DbServiceImpl implements DbService {
    private DataSource dataSource;

    private static final String SQL_REFRESH_TRANSACTION_MATERIALIZED_VIEW = "REFRESH MATERIALIZED VIEW expenses.multicurrency_transactions";

    public DbServiceImpl(@Autowired DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void refreshTransactionsMaterializedView() throws SQLException {
        dataSource.getConnection().prepareCall(SQL_REFRESH_TRANSACTION_MATERIALIZED_VIEW).execute();
    }
}
