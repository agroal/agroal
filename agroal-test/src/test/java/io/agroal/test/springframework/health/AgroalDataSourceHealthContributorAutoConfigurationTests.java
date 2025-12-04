// Copyright (C) 2023 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.springframework.health;

import io.agroal.springframework.boot.AgroalDataSource;
import io.agroal.springframework.boot.AgroalDataSourceAutoConfiguration;
import io.agroal.springframework.boot.health.AgroalDataSourceHealthContributorAutoConfiguration;
import io.agroal.springframework.boot.health.AgroalDataSourceHealthIndicator;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.datasource.DelegatingDataSource;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import static io.agroal.test.AgroalTestGroup.SPRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.fail;

@Tag( SPRING )
class AgroalDataSourceHealthContributorAutoConfigurationTests {

    private static final Logger LOG = LoggerFactory.getLogger(AgroalDataSourceHealthContributorAutoConfigurationTests.class);

    private final Class<?>[] autoconfigurationsInWrongOrder = new Class<?>[]{
            DataSourceAutoConfiguration.class,
            AgroalDataSourceAutoConfiguration.class,
            AgroalDataSourceHealthContributorAutoConfiguration.class
    };

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(autoconfigurationsInWrongOrder));

    @DisplayName("AutoConfiguration will create AgroalDataSourceHealthIndicator")
    @Test
    void testHealthCheckEnabled() {
        runner.run(context -> {
                  assertThat(context).hasSingleBean(AgroalDataSourceHealthIndicator.class);

                  AgroalDataSourceHealthIndicator agroalDataSourceHealthIndicator = context.getBean(AgroalDataSourceHealthIndicator.class);
                  Health health = agroalDataSourceHealthIndicator.health(true);
                  LOG.info("Got health {}", health);
                  assertThat(health.getStatus()).isEqualTo(Status.UP);

                  AgroalDataSource dataSource = context.getBean(AgroalDataSource.class);
                  try (Connection c = dataSource.getConnection()) {
                     LOG.info("Got connection {}", c);
                  }
              });
    }

    @DisplayName("AutoConfiguration will create AgroalDataSourceHealthIndicator for AgroalDataSource with strict transactionRequirement")
    @Test
    void testHealthCheckEnabledWithStrictTransactionRequirement() {
        runner.withPropertyValues("spring.datasource.agroal.transactionRequirement=strict")
                .run(context -> {
                    assertThat(context).hasSingleBean(AgroalDataSourceHealthIndicator.class);

                    AgroalDataSourceHealthIndicator agroalDataSourceHealthIndicator = context.getBean(AgroalDataSourceHealthIndicator.class);
                    Health health = agroalDataSourceHealthIndicator.health(true);
                    LOG.info("Got health {}", health);
                    assertThat(health.getStatus()).isEqualTo(Status.UP);

                    AgroalDataSource dataSource = context.getBean(AgroalDataSource.class);
                    try (Connection connection = dataSource.getConnection()) {
                        fail("Unexpected got connection: " + connection);
                    } catch (SQLException e) {
                        LOG.info("Got SQLException: {}", e.getMessage());
                        assertThat(e.getMessage()).isEqualTo("Connection acquired without transaction.");
                    }
                });
    }

    @DisplayName("AutoConfiguration will create AgroalDataSourceHealthIndicator for wrapped AgroalDataSource")
    @Test
    void testHealthCheckEnabledWithWrappedAgroalDataSources() throws SQLException {
        AgroalDataSource ds = new AgroalDataSource();
        ds.setDriverClass(JdbcDataSource.class);
        ds.setUrl("jdbc:h2:mem:ds1;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false");
        ds.setUsername("sa");
        ds.afterPropertiesSet();

        runner.withBean("dataSource", DataSource.class, () -> new DelegatingDataSource(ds))
                .run(context ->  {
                    assertThat(context).hasSingleBean(AgroalDataSourceHealthIndicator.class);

                    AgroalDataSourceHealthIndicator agroalDataSourceHealthIndicator = context.getBean(AgroalDataSourceHealthIndicator.class);
                    Health health = agroalDataSourceHealthIndicator.health(true);
                    LOG.info("Got health {}", health);
                    assertThat(health.getStatus()).isEqualTo(Status.UP);

                    DataSource dataSource = context.getBean(DataSource.class);
                    try (Connection c = dataSource.getConnection()) {
                        LOG.info("Got connection {}", c);
                    }
                });
    }

    @DisplayName("AutoConfiguration will create HealthIndicator instances for AgroalDataSource and plain DataSource")
    @Test
    void testHealthCheckEnabledWithMultipleDataSources() {
        AgroalDataSource ds1 = new AgroalDataSource();
        ds1.setDriverClass(JdbcDataSource.class);
        ds1.setUrl("jdbc:h2:mem:ds1;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false");
        ds1.setUsername("sa");
        JdbcDataSource ds2 = new JdbcDataSource();
        ds2.setUrl("jdbc:h2:mem:ds2;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false");
        ds2.setUser("sa");

        runner.withBean("ds1", AgroalDataSource.class, () -> ds1)
                .withBean("ds2", DataSource.class, () -> ds2)
                .run(context ->  {
                    assertThat(context).hasSingleBean(CompositeHealthContributor.class);

                    CompositeHealthContributor compositeHealthContributor = context.getBean(CompositeHealthContributor.class);
                    assertThat(compositeHealthContributor.stream().count()).isEqualTo(2);
                    compositeHealthContributor.forEach(contributor -> {
                        Health health = ((HealthIndicator) contributor.contributor()).health(true);
                        LOG.info("Got health {}", health);
                        assertThat(health.getStatus()).isEqualTo(Status.UP);
                    });

                    for (DataSource dataSource : context.getBeanProvider(DataSource.class)) {
                        try (Connection c = dataSource.getConnection()) {
                            LOG.info("Got connection {}", c);
                        }
                    }
                });
    }

    @DisplayName("AutoConfiguration won't create AgroalDataSourceHealthIndicator")
    @Test
    void testHealthCheckDisabled() {
        runner.withPropertyValues("management.health.db.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(AgroalDataSourceHealthIndicator.class);

                    AgroalDataSource dataSource = context.getBean(AgroalDataSource.class);
                    try (Connection c = dataSource.getConnection()) {
                        LOG.info("Got connection {}", c);
                    }
                });
    }
}
