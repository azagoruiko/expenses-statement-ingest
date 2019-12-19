package ua.org.zagoruiko.expenses.spark.etl.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@PropertySource(value = "classpath:application.properties")
public class SpreadsheetsServiceConfig {
    @Value("${spreadsheets.base_url}")
    private String baseUrl;

    public String getBaseUrl() {
        return baseUrl;
    }
}
