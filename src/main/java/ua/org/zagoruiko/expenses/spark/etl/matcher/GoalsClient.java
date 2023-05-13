package ua.org.zagoruiko.expenses.spark.etl.matcher;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.glassfish.jersey.client.ClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ua.org.zagoruiko.expenses.spark.etl.config.GoalsServiceConfig;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;

@Component
public class GoalsClient {
    public static class FamilyReportRequestEventDTO implements Serializable {
        private String family;
        private String message;

        public FamilyReportRequestEventDTO() {
        }

        public String getFamily() {
            return family;
        }

        public String getMessage() {
            return message;
        }

        public void setFamily(String family) {
            this.family = family;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }


    private GoalsServiceConfig serviceConfig;

    private ClientConfig cfg = new ClientConfig();
    private CloseableHttpClient client;

    @Autowired
    public GoalsClient(GoalsServiceConfig serviceConfig) {
        this.serviceConfig = serviceConfig;
        this.client = HttpClients.createDefault();
    }

    public void notifyGoals() {
        FamilyReportRequestEventDTO dto = new FamilyReportRequestEventDTO();
        dto.setFamily("zagoruiko");
        dto.setMessage("Я щойно порахував статистику витрат за поточний мiсяць!\n" +
                "Якщо ти вчасно завантажив/ла усi виписки, то данi мають бути свiженькими!\n");

        HttpPost request = new HttpPost(this.serviceConfig.getBaseUrl() + "event/bot/report/family");
        request.setHeader("Accept", "application/json");
        request.setHeader("Content-type", "application/json; charset=UTF-8");

        try {
            ObjectMapper om = new ObjectMapper();
            request.setEntity(new StringEntity(om.writeValueAsString(dto), Charset.forName("UTF-8")));
            CloseableHttpResponse response = this.client.execute(request);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
