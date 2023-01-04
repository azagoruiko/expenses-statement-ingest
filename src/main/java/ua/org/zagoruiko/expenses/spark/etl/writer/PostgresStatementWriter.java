package ua.org.zagoruiko.expenses.spark.etl.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;

import static org.apache.spark.sql.functions.col;

@Component("pgJdbcWriter")
public class PostgresStatementWriter implements StatementWriter {
    @Autowired
    @Qualifier("postgres.jdbc.url")
    private String jdbcUrl;

    @Autowired
    @Qualifier("postgres.jdbc.driver")
    private String jdbcDriver;

    @Autowired
    @Qualifier("postgres.jdbc.user")
    private String jdbcUser;

    @Autowired
    @Qualifier("postgres.jdbc.password")
    private String jdbcPassword;

    @Value("${postgres.jdbc.table}")
    private String jdbcTable = "transactions";

    @Override
    public void write(Dataset<Row> dataset) {
        Properties jdbcProperties = new Properties();
        jdbcProperties.setProperty("driver", this.jdbcDriver);
        jdbcProperties.setProperty("user", this.jdbcUser);
        jdbcProperties.setProperty("password", this.jdbcPassword);
        jdbcProperties.setProperty("truncate", "true");
        jdbcProperties.setProperty("batchsize", "10000");
        jdbcProperties.setProperty("isolationLevel", "NONE");
        dataset.select(col("id"),
                col("date_time").as("transaction_date"),
                col("amount"),
                col("currency"),
                col("operation").as("description"),
                col("tags"),
                col("category"))
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc(this.jdbcUrl, this.jdbcTable, jdbcProperties);

    }
}
