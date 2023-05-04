package ua.org.zagoruiko.expenses.spark.etl.loader;

import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ua.org.zagoruiko.expenses.spark.etl.dto.SpreadsheetExpenseDTO;
import ua.org.zagoruiko.expenses.spark.etl.matcher.SpreadsheetsClient;

import java.util.List;

@Service("spreadsheetsStatementsRawLoader")
public class SpreadsheetsStatementRawLoader implements StatementLoader {
    private SparkSession spark;
    private SpreadsheetsClient spreadsheetsClient;

    @Autowired
    public SpreadsheetsStatementRawLoader(SparkSession spark, SpreadsheetsClient spreadsheetsClient) {
        this.spark = spark;

        this.spreadsheetsClient = spreadsheetsClient;

    }

    @Override
    public Dataset<Row> load() {
        List<SpreadsheetExpenseDTO> expenses = this.spreadsheetsClient.getExpenses();
        int ids = 1;

        for (SpreadsheetExpenseDTO dto : expenses) {
            dto.setId(ids++);
            dto.setAmount(dto.getAmount() * (-1f));
        }

        Dataset<Row> dsToWrite = spark.sqlContext()
                .createDataset(expenses, Encoders.bean(SpreadsheetExpenseDTO.class))
                .withColumn("id", functions.concat_ws("-",
                        functions.lit("spreadsheets"),
                        functions.col("id")))
                .withColumn("operation", functions.concat_ws(":",
                        functions.col("source"),
                        functions.col("category")))
                ;
        dsToWrite.repartition(1)
                .write().mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv("s3a://spreadsheet.archive/");

        return dsToWrite;
    }
}
