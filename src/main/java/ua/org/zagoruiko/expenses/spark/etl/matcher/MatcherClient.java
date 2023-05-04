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
import ua.org.zagoruiko.expenses.matcherservice.dto.CategoryTagDTO;
import ua.org.zagoruiko.expenses.matcherservice.dto.MatcherSetDTO;
import ua.org.zagoruiko.expenses.spark.etl.config.MatcherServiceConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Component
public class MatcherClient {
    private MatcherServiceConfig serviceConfig;

    private ClientConfig cfg = new ClientConfig();
    private CloseableHttpClient client;

    @Autowired
    public MatcherClient(MatcherServiceConfig serviceConfig) {
        this.serviceConfig = serviceConfig;
        this.client = HttpClients.createDefault();
    }

    public List<String> getTags(String operation) {
        HttpGet request = new HttpGet(this.serviceConfig.getBaseUrl() + "/matchers/match");
        try {
            CloseableHttpResponse response = this.client.execute(request);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                // return it as a String
                ObjectMapper om = new ObjectMapper();
                return Arrays.asList(om.readValue(EntityUtils.toString(entity), String[].class));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public MatcherSetDTO getMatchers(String privider) {
        HttpGet request = new HttpGet(this.serviceConfig.getBaseUrl() + "/matchers/" + privider);
        try {
            CloseableHttpResponse response = this.client.execute(request);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                // return it as a String
                ObjectMapper om = new ObjectMapper();
                return om.readValue(EntityUtils.toString(entity), MatcherSetDTO.class);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public MatcherSetDTO getMatchers() {
        HttpGet request = new HttpGet(this.serviceConfig.getBaseUrl() + "/matchers/");
        try {
            CloseableHttpResponse response = this.client.execute(request);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                // return it as a String
                ObjectMapper om = new ObjectMapper();
                return om.readValue(EntityUtils.toString(entity), MatcherSetDTO.class);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public List<CategoryTagDTO> getCategoryTags() {
        HttpGet request = new HttpGet(this.serviceConfig.getBaseUrl() + "/tags/categories");
        try {
            CloseableHttpResponse response = this.client.execute(request);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                // return it as a String
                ObjectMapper om = new ObjectMapper();
                return Arrays.asList(om.readValue(EntityUtils.toString(entity), CategoryTagDTO[].class));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
