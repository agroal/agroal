// Copyright (C) 2023 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.springframework.health;

import io.agroal.springframework.boot.AgroalDataSource;
import io.agroal.springframework.boot.AgroalDataSourceAutoConfiguration;
import io.agroal.springframework.boot.health.AgroalDataSourceHealthContributorAutoConfiguration;
import io.agroal.springframework.boot.health.AgroalDataSourceHealthIndicator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.sql.Connection;
import java.sql.SQLException;

import static io.agroal.test.AgroalTestGroup.SPRING;
import static org.assertj.core.api.Assertions.assertThat;
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
                  Health health = agroalDataSourceHealthIndicator.getHealth(true);
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
                    Health health = agroalDataSourceHealthIndicator.getHealth(true);
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
