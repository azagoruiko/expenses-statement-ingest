package ua.org.zagoruiko.expenses.spark.etl.matcher;

import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ua.org.zagoruiko.expenses.matcherservice.dto.MatcherSetDTO;
import ua.org.zagoruiko.expenses.spark.etl.config.GoalsServiceConfig;
import ua.org.zagoruiko.expenses.spark.etl.config.MatcherServiceConfig;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

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
    private Client client;

    @Autowired
    public GoalsClient(GoalsServiceConfig serviceConfig) {
        this.serviceConfig = serviceConfig;
        this.cfg.register(JacksonJsonProvider.class);
        this.client = ClientBuilder.newBuilder().withConfig(this.cfg).build();
    }

    public void notifyGoals() {
        WebTarget target = client.target(this.serviceConfig.getBaseUrl() + "event/bot/report/family");
        Invocation.Builder ib = target.request(MediaType.APPLICATION_JSON);
        FamilyReportRequestEventDTO dto = new FamilyReportRequestEventDTO();
        dto.setFamily("zagoruiko");
        dto.setMessage("Я щойно порахував статистику витрат за поточний мiсяць!\n" +
                "Якщо ти вчасно завантажив/ла усi виписки, то данi мають бути свiженькими!\n");
        ib.post(Entity.entity(dto,
                MediaType.APPLICATION_JSON_TYPE));
    }
}
