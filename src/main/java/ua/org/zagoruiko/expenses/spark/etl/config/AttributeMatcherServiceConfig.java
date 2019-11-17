package ua.org.zagoruiko.expenses.spark.etl.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
//import retrofit2.Retrofit;
//import retrofit2.converter.gson.GsonConverterFactory;
//import ua.org.zagoruiko.expenses.matcherservice.client.retrofit.MatcherServiceClient;
import ua.org.zagoruiko.expenses.matcherservice.dto.MatcherSetDTO;

import java.io.IOException;

@Configuration
public class AttributeMatcherServiceConfig {
    @Value("${matcher.service.base_url}")
    private String baseUrl;

//    @Bean
//    public MatcherServiceClient client() throws IOException {
//        Gson gson = new GsonBuilder()
//                .create();
//
//        Retrofit retrofit = new Retrofit.Builder()
//                .baseUrl(this.baseUrl)
//                .addConverterFactory(GsonConverterFactory.create(gson))
//                .build();
//
//        return retrofit.create(MatcherServiceClient.class);
//    }
}
