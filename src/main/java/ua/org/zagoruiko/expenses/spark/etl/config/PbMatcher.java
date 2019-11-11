package ua.org.zagoruiko.expenses.spark.etl.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ua.org.zagoruiko.expenses.category.matcher.Matcher;
import ua.org.zagoruiko.expenses.category.model.Tag;
import ua.org.zagoruiko.expenses.category.resolver.CategoryContainsResolver;
import ua.org.zagoruiko.expenses.category.resolver.ExactCategoryResolver;
import ua.org.zagoruiko.expenses.category.resolver.TagsContainsResolver;
import ua.org.zagoruiko.expenses.matcherservice.client.retrofit.MatcherServiceClient;
import ua.org.zagoruiko.expenses.matcherservice.dto.CategoryMatcherDTO;
import ua.org.zagoruiko.expenses.matcherservice.dto.MatcherSetDTO;
import ua.org.zagoruiko.expenses.matcherservice.dto.TagsMatcherDTO;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Configuration
public class PbMatcher {
    @Autowired
    private MatcherServiceClient matcherServiceClient;

    @Bean(name = "pb_matcher")
    public Matcher matcher2() throws IOException {
        MatcherSetDTO matcherSet = this.matcherServiceClient.matchers("pb").execute().body();

        final Map<String, String> categoryMatch = new HashMap<>();
        final Map<String, String> categoryContains = new HashMap<>();
        final Map<String, Set<Tag>> tagMatch = new HashMap<>();
        final Map<String, Set<Tag>> tagContains = new HashMap<>();


        for (CategoryMatcherDTO matcher : matcherSet.getCategoryMatcher()) {
            switch (matcher.getFunc()) {
                case "EQ":
                    categoryMatch.put(matcher.getPattern(), matcher.getCategory());
                    break;
                case "CONTAINS":
                    categoryContains.put(matcher.getPattern(), matcher.getCategory());
                    break;
            }
        }

        for (TagsMatcherDTO matcher : matcherSet.getTagsMatcher()) {
            switch (matcher.getFunc()) {
                case "EQ":
                    tagMatch.put(matcher.getPattern(), matcher.getTags().stream()
                            .map(tag -> Tag.valueOf(tag)).collect(Collectors.toSet()));
                    break;
                case "CONTAINS":
                    tagContains.put(matcher.getPattern(), matcher.getTags().stream()
                            .map(tag -> Tag.valueOf(tag)).collect(Collectors.toSet()));
                    break;
            }
        }

        ExactCategoryResolver categoryResolver = new ExactCategoryResolver(categoryMatch);
        CategoryContainsResolver categoryContainsResolver = new CategoryContainsResolver(categoryContains);

        TagsContainsResolver tagsContainsResolver = new TagsContainsResolver(tagContains);
        TagsContainsResolver tagsMatchResolver = new TagsContainsResolver(tagMatch);

        return new ua.org.zagoruiko.expenses.category.matcher.PbMatcher(
                Arrays.asList(categoryResolver, categoryContainsResolver),
                Arrays.asList(tagsMatchResolver, tagsContainsResolver));
    }

    @Bean(name = "pb_matcher_stub")
    public Matcher matcher() {
        final Map<String, String> categoryMatch = new HashMap<>();
        final Map<String, String> categoryContains = new HashMap<>();
        final Map<String, Set<Tag>> tagContains = new HashMap<>();

        categoryMatch.put("Продукты питания", "Food and Drinks");
        categoryMatch.put("Кафе, бары, рестораны", "Cafe and Restaurants");
        categoryMatch.put("Развлечения", "Entertainment");
        categoryMatch.put("Коммунальные услуги", "Utilities");
        categoryMatch.put("Пополнение мобильного", "Utilities");

        categoryContains.put("WFPTAXI", "Transport");
        categoryContains.put("fozzy", "Food and Drinks");
        categoryContains.put("Ресторан", "Cafe and Restaurants");
        categoryContains.put("PAMPIK", "Kids");
        categoryContains.put("ANTOSHKA", "Kids");
        categoryContains.put("Медицина", "Healthcare");
        categoryContains.put("Аптека", "Healthcare");

        tagContains.put("WFPTAXI", new HashSet<>(Arrays.asList(new Tag[] {
                Tag.TAG_TAXI,
                Tag.TAG_TRANSPORT})));

        tagContains.put("fozzy", new HashSet<>(Arrays.asList(new Tag[] {
                Tag.TAG_FOZZY,
                Tag.TAG_SUPERMARKET,
                Tag.TAG_HAS_DRINKS
        })));

        tagContains.put("YIDALNYA", new HashSet<>(Arrays.asList(new Tag[] {
                Tag.TAG_YIDALNYA,
                Tag.TAG_EAT_OUT
        })));

        ExactCategoryResolver categoryResolver = new ExactCategoryResolver(categoryMatch);

        CategoryContainsResolver categoryContainsResolver = new CategoryContainsResolver(categoryContains);
        TagsContainsResolver tagsContainsResolver = new TagsContainsResolver(tagContains);

        return new ua.org.zagoruiko.expenses.category.matcher.PbMatcher(
                Arrays.asList(categoryResolver, categoryContainsResolver),
                Arrays.asList(tagsContainsResolver));
    }
}
