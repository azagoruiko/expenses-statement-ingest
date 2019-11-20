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

@Component("jdbcWriter")
public class JdbcStatementWriter implements StatementWriter {
    @Autowired
    @Qualifier("jdbc.url")
    private String jdbcUrl;

    @Autowired
    @Qualifier("jdbc.driver")
    private String jdbcDriver;

    @Autowired
    @Qualifier("jdbc.user")
    private String jdbcUser;

    @Autowired
    @Qualifier("jdbc.password")
    private String jdbcPassword;

    @Value("${jdbc.table}")
    private String jdbcTable = "transactions";

    @Override
    public void write(Dataset<Row> dataset) {
        Properties jdbcProperties = new Properties();
        jdbcProperties.setProperty("driver", this.jdbcDriver);
        jdbcProperties.setProperty("user", this.jdbcUser);
        jdbcProperties.setProperty("password", this.jdbcPassword);
        dataset.select(col("category"),
                functions.concat_ws(" ", col("date"), col("time")).as("transaction_time"),
                col("amount"),
                col("amount_orig"),
                col("operation"),
                col("tags"))
                .write()
                .mode(SaveMode.Append)
                .jdbc(this.jdbcUrl, this.jdbcTable, jdbcProperties);
    }
}
