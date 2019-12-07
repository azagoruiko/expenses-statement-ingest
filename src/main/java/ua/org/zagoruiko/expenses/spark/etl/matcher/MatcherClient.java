package ua.org.zagoruiko.expenses.spark.etl.matcher;

import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import ua.org.zagoruiko.expenses.matcherservice.dto.MatcherSetDTO;
import ua.org.zagoruiko.expenses.spark.etl.config.MatcherServiceConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.List;

@Component
public class MatcherClient {
    private MatcherServiceConfig serviceConfig;

    private ClientConfig cfg = new ClientConfig();
    private Client client;

    @Autowired
    public MatcherClient(MatcherServiceConfig serviceConfig) {
        this.serviceConfig = serviceConfig;
        this.cfg.register(JacksonJsonProvider.class);
        this.client = ClientBuilder.newBuilder().withConfig(this.cfg).build();
    }

    public List<String> getTags(String operation) {
        WebTarget target = client.target(this.serviceConfig.getBaseUrl() + "/matchers/match")
                .queryParam("text", operation);
        Invocation.Builder ib = target.request(MediaType.APPLICATION_JSON);
        return Arrays.asList(ib.get( String[].class));
    }

    public MatcherSetDTO getMatchers(String privider) {
        WebTarget target = client.target(this.serviceConfig.getBaseUrl() + "/matchers/" + privider);
        Invocation.Builder ib = target.request(MediaType.APPLICATION_JSON);
        return ib.get( MatcherSetDTO.class);
    }

    public MatcherSetDTO getMatchers() {
        WebTarget target = client.target(this.serviceConfig.getBaseUrl() + "/matchers/");
        Invocation.Builder ib = target.request(MediaType.APPLICATION_JSON);
        return ib.get( MatcherSetDTO.class);
    }
}
