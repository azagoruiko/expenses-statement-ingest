package ua.org.zagoruiko.expenses.spark.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.codehaus.jackson.type.TypeReference;
import org.glassfish.jersey.client.ClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import ua.org.zagoruiko.expenses.category.matcher.Matcher;
import ua.org.zagoruiko.expenses.spark.etl.loader.StatementLoader;
import ua.org.zagoruiko.expenses.spark.etl.writer.StatementWriter;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.Serializable;
import java.net.URLEncoder;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

@Component
@PropertySource(value = "classpath:application.properties")
public class ImportPb implements Serializable {
    public static final long serialVersionUID = 0L;

    @Autowired
    @Qualifier("taggedLoader")
    private StatementLoader loader;

    @Autowired
    @Qualifier("savedLoader")
    private StatementLoader savedLoader;

    @Autowired
    @Qualifier("rawWriter")
    private StatementWriter rawWriter;

    @Autowired
    @Qualifier("jdbcWriter")
    private StatementWriter jdbcWriter;

    @Autowired
    @Qualifier("csvWriter")
    private StatementWriter csvWriter;

    @Autowired
    @Qualifier("taggedWriter")
    private StatementWriter taggedWriter;

    @Autowired
    private SparkSession spark;

    public static void main(String[] args) throws Exception {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.scan(ImportPb.class.getPackage().getName());
        context.refresh();
        context.getBean(ImportPb.class).run(args);
    }

    public void run(String[] args) throws Exception {


        //create table transactions ( id int primary key auto_increment, transaction_date datetime, category varchar(200), category_id int, amount decimal(13, 2), amount_orig varchar(20), operation varchar(1024), tags varchar(2048));

        Dataset<Row> ds = this.loader.load();

        this.rawWriter.write(ds);
        this.jdbcWriter.write(ds);
        this.taggedWriter.write(ds);

        this.csvWriter.write(this.savedLoader.load());
        spark.stop();
    }
}
