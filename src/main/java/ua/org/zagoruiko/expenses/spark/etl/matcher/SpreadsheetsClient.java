package ua.org.zagoruiko.expenses.spark.etl.matcher;

import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ua.org.zagoruiko.expenses.matcherservice.dto.CategoryTagDTO;
import ua.org.zagoruiko.expenses.matcherservice.dto.MatcherSetDTO;
import ua.org.zagoruiko.expenses.spark.etl.config.MatcherServiceConfig;
import ua.org.zagoruiko.expenses.spark.etl.config.SpreadsheetsServiceConfig;
import ua.org.zagoruiko.expenses.spark.etl.dto.SpreadsheetExpenseDTO;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
public class SpreadsheetsClient {
    private SpreadsheetsServiceConfig serviceConfig;

    private ClientConfig cfg = new ClientConfig();
    private Client client;

    @Autowired
    public SpreadsheetsClient(SpreadsheetsServiceConfig serviceConfig) {
        this.serviceConfig = serviceConfig;
        this.cfg.register(JacksonJsonProvider.class);
        this.client = ClientBuilder.newBuilder().withConfig(this.cfg).build();
    }

    public List<SpreadsheetExpenseDTO> getExpenses() {
        WebTarget target = client.target(this.serviceConfig.getBaseUrl() + "/expenses");
        Invocation.Builder ib = target.request(MediaType.APPLICATION_JSON);
        return Arrays.asList(ib.get( SpreadsheetExpenseDTO[].class));
    }

}
