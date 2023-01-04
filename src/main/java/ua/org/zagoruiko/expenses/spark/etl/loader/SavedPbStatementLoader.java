package ua.org.zagoruiko.expenses.spark.etl.loader;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

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
