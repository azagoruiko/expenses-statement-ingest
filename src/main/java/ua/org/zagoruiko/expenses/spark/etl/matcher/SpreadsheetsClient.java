package ua.org.zagoruiko.expenses.spark.etl.matcher;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.glassfish.jersey.client.ClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ua.org.zagoruiko.expenses.spark.etl.config.SpreadsheetsServiceConfig;
import ua.org.zagoruiko.expenses.spark.etl.dto.SpreadsheetExpenseDTO;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Component
public class SpreadsheetsClient {
    private SpreadsheetsServiceConfig serviceConfig;

    private ClientConfig cfg = new ClientConfig();
    private CloseableHttpClient client;

    @Autowired
    public SpreadsheetsClient(SpreadsheetsServiceConfig serviceConfig) {
        this.serviceConfig = serviceConfig;
        this.client = HttpClients.createDefault();
    }

    public List<SpreadsheetExpenseDTO> getExpenses() {
        HttpGet request = new HttpGet(this.serviceConfig.getBaseUrl() + "/expenses");
        try {
            CloseableHttpResponse response = this.client.execute(request);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                // return it as a String
                ObjectMapper om = new ObjectMapper();
                return Arrays.asList(om.readValue(EntityUtils.toString(entity), SpreadsheetExpenseDTO[].class));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

}
