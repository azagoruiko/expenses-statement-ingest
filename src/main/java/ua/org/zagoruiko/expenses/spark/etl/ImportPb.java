package ua.org.zagoruiko.expenses.spark.etl;

import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;
import ua.org.zagoruiko.expenses.spark.etl.loader.StatementLoader;
import ua.org.zagoruiko.expenses.spark.etl.writer.StatementWriter;

import java.io.Serializable;

@Component
@PropertySource(value = "classpath:application.properties")
public class ImportPb implements Serializable {
    public static final long serialVersionUID = 0L;

    @Autowired
    @Qualifier("rawLoader")
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
    @Qualifier("taggedCsvWriter")
    private StatementWriter taggedCsvWriter;

    @Autowired
    @Qualifier("taggedArrayWriter")
    private StatementWriter taggedArrayWriter;

    @Autowired
    private SparkSession spark;

    public static void main(String[] args) throws Exception {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.scan(ImportPb.class.getPackage().getName());
        context.refresh();
        context.getBean(ImportPb.class).run(args);
    }

    public void run(String[] args) throws Exception {
        Dataset<Row> ds = this.loader.load();

        this.rawWriter.write(ds);
        //this.jdbcWriter.write(ds);
        //this.taggedWriter.write(ds);

        //this.csvWriter.write(this.savedLoader.load());
        //this.taggedCsvWriter.write(this.savedLoader.load());
        this.taggedArrayWriter.write(this.savedLoader.load());
        spark.stop();
    }
}
