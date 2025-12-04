// Copyright (C) 2023 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.springframework;

import io.agroal.springframework.boot.AgroalDataSourceAutoConfiguration;
import liquibase.integration.spring.SpringLiquibase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.liquibase.autoconfigure.LiquibaseAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import javax.sql.DataSource;

import static io.agroal.test.AgroalTestGroup.SPRING;
import static org.assertj.core.api.Assertions.assertThat;

@Tag( SPRING )
class LiquibaseTests {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    AgroalDataSourceAutoConfiguration.class,
                    DataSourceAutoConfiguration.class,
                    LiquibaseAutoConfiguration.class
            ));

    @DisplayName("Liquibase will work when separate credentials are provided for liquibase")
    @Test
    void testAutoconfigureLiquibaseUsingCustomCredentials() {
        runner.withPropertyValues(
                        "spring.liquibase.user=liquibaseuser",
                        "spring.liquibase.password=liquibasepassword")
                .run(context -> {
                    assertThat(context).hasSingleBean(SpringLiquibase.class);
                    DataSource primaryDataSource = context.getBean(DataSource.class);
                    DataSource liquibaseDataSource = context.getBean(SpringLiquibase.class).getDataSource();

                    assertThat(liquibaseDataSource)
                            .isNotSameAs(primaryDataSource)
                            .hasFieldOrPropertyWithValue("username", "liquibaseuser")
                            .hasFieldOrPropertyWithValue("password", "liquibasepassword");

                });
    }
}
