package ua.org.zagoruiko.expenses.spark.etl.matcher;

import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.glassfish.jersey.client.ClientConfig;
import ua.org.zagoruiko.expenses.matcherservice.dto.MatcherSetDTO;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.List;

public class MatcherClient {
    public List<String> getTags(String operation) {
        ClientConfig cfg = new ClientConfig();
        cfg.register(JacksonJsonProvider.class);
        Client client = ClientBuilder.newBuilder().withConfig(cfg).build();
        WebTarget target = client.target("http://192.168.0.101:8080/matchers/match")
                .queryParam("text", operation);
        Invocation.Builder ib = target.request(MediaType.APPLICATION_JSON);
        return Arrays.asList(ib.get( String[].class));
    }

    public MatcherSetDTO getMatchers(String privider) {
        ClientConfig cfg = new ClientConfig();
        cfg.register(JacksonJsonProvider.class);
        Client client = ClientBuilder.newBuilder().withConfig(cfg).build();
        WebTarget target = client.target("http://192.168.0.101:8080/matchers/" + privider);
        Invocation.Builder ib = target.request(MediaType.APPLICATION_JSON);
        return ib.get( MatcherSetDTO.class);
    }
}
