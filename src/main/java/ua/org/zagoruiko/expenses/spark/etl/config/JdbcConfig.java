package ua.org.zagoruiko.expenses.spark.etl.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@Configuration
public class JdbcConfig {
    @Value("${jdbc.url}")
    String jdbcUrl;

    @Value("${jdbc.driver}")
    String jdbcDriver;

    @Value("${jdbc.user}")
    String jdbcUser;

    @Value("${jdbc.password}")
    String jdbcPassword;

    @Bean(name = "jdbc.url")
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    @Bean(name = "jdbc.driver")
    public String getJdbcDriver() {
        return jdbcDriver;
    }

    @Bean(name = "jdbc.user")
    public String getJdbcUser() {
        return jdbcUser;
    }

    @Bean(name = "jdbc.password")
    public String getJdbcPassword() {
        return jdbcPassword;
    }

    @Bean(name = "mysqlDataSource")
    public DataSource getDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(this.jdbcDriver);
        dataSource.setUrl(this.jdbcUrl);
        dataSource.setUsername(this.jdbcUser);
        dataSource.setPassword(this.jdbcPassword);

        return dataSource;
    }

    @Bean
    public JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(this.getDataSource());
    }
}
