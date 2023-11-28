// Copyright (C) 2023 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.springframework;

import dev.snowdrop.boot.narayana.autoconfigure.NarayanaConfiguration;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.transaction.TransactionIntegration;
import io.agroal.narayana.NarayanaTransactionIntegration;
import io.agroal.springframework.boot.AgroalDataSource;
import io.agroal.springframework.boot.AgroalDataSourceConfiguration;
import io.agroal.springframework.boot.metrics.AgroalDataSourcePoolMetadata;
import io.agroal.test.MockXADataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.annotation.UserConfigurations;
import org.springframework.boot.jdbc.metadata.DataSourcePoolMetadata;
import org.springframework.boot.jdbc.metadata.DataSourcePoolMetadataProvider;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.naming.InitialContext;
import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

import static io.agroal.test.AgroalTestGroup.SPRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag( SPRING )
class AgroalDataSourceConfigurationTests {

    private static final Logger LOG = LoggerFactory.getLogger(AgroalDataSourceConfigurationTests.class);

    private final Class<?>[] autoconfigurationsInWrongOrder = new Class<?>[]{
            DataSourceAutoConfiguration.class,
            AgroalDataSourceConfiguration.class
    };

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(autoconfigurationsInWrongOrder));

    @DisplayName("Autoconfigurations will trigger in the correct order and the beans will be registered and created")
    @Test
    void testAutoconfigureAgroalDataSourceWithMetrics() {
        runner.withPropertyValues("spring.datasource.agroal.metrics=true")
                .run(context -> {
            assertThat(context).hasSingleBean(AgroalDataSource.class);

            DataSource dataSource = context.getBean(AgroalDataSource.class);
            Map<String, DataSourcePoolMetadataProvider> metadataProviders = context.getBeansOfType(DataSourcePoolMetadataProvider.class);

            DataSourcePoolMetadata metadata = metadataProviders.values().stream()
                    .map(provider -> provider.getDataSourcePoolMetadata(dataSource))
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("AgroalDataSourcePoolMetaData not provided by any DataSourcePoolMetaDataProvider"));
            assertThat(metadata).isInstanceOf(AgroalDataSourcePoolMetadata.class);
            assertThat(metadata.getActive()).isEqualTo(0);

            try (Connection c = dataSource.getConnection()) {
                LOG.info("Got connection {}", c);
                assertThat(metadata.getActive()).isEqualTo(1);
            }
        });
    }

    @DisplayName("Autoconfiguration will create DataSource with provided properties")
    @Test
    void testAutoconfigureAgroalDataSourceWithProperties() {
        runner.withPropertyValues(
                "spring.datasource.agroal.min-size=13",
                "spring.datasource.agroal.initial-size=7",
                "spring.datasource.agroal.max-size=37")
                .run(context -> {
                    AgroalDataSource dataSource = context.getBean(AgroalDataSource.class);
                    AgroalConnectionPoolConfiguration poolConfiguration = dataSource.getConfiguration().connectionPoolConfiguration();

                    assertThat(poolConfiguration.minSize()).isEqualTo(13);
                    assertThat(poolConfiguration.initialSize()).isEqualTo(7);
                    assertThat(poolConfiguration.maxSize()).isEqualTo(37);

                    try (Connection c = dataSource.getConnection()) {
                        LOG.info("Got connection {}", c);
                    }
                });
    }

    @DisplayName("Autoconfiguration will create DataSource with integration to Narayana")
    @Test
    void testAutoconfigureAgroalDataSourceWithNarayanaIntegration() {
        runner.withPropertyValues("narayana.logDir=ObjectStore")
                .withConfiguration(UserConfigurations.of(NarayanaConfiguration.class))
                .run(context -> {
                    AgroalDataSource dataSource = context.getBean(AgroalDataSource.class);
                    TransactionIntegration transactionIntegration = dataSource.getConfiguration().connectionPoolConfiguration().transactionIntegration();

                    assertThat(transactionIntegration).isInstanceOf(NarayanaTransactionIntegration.class);

                    JtaTransactionManager txManager = context.getBean(JtaTransactionManager.class);
                    assertThat(txManager).isNotNull();

                    TransactionStatus txStatus = txManager.getTransaction(null);
                    assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isTrue();

                    Connection c = dataSource.getConnection();
                    LOG.info("Got connection {}", c);

                    assertThatThrownBy(() -> c.setAutoCommit( true )).isInstanceOf(SQLException.class);
                    assertThat(c.getAutoCommit()).isFalse();
                    assertThat(c.unwrap(Connection.class)).isEqualTo(dataSource.getConnection().unwrap(Connection.class));

                    txManager.rollback(txStatus);
                    assertThat(c.isClosed()).isTrue();
                });
    }

    @DisplayName("Autoconfiguration will create connectable DataSource bound to JNDI")
    @Test
    void testAutoconfigureAgroalDataSourceBoundToJndi() {
        System.setProperty("java.naming.factory.initial", "org.osjava.sj.SimpleJndiContextFactory");
        System.setProperty("org.osjava.sj.jndi.shared", "true");
        System.setProperty("org.osjava.sj.jndi.ignoreClose", "true");
        runner.withConfiguration(UserConfigurations.of(NarayanaConfiguration.class))
                .withPropertyValues(
                        "narayana.logDir=ObjectStore",
                        "spring.datasource.agroal.connectable=true",
                        "spring.datasource.jndi-name=java:comp/env/jdbc/test")
                .run(context -> {
                    InitialContext ic = new InitialContext();
                    Object dataSource = ic.lookup("java:comp/env/jdbc/test");

                    assertThat(dataSource).isNotNull();
                    assertThat(dataSource).isInstanceOf(AgroalDataSource.class);

                    try (Connection c = ((AgroalDataSource) dataSource).getConnection()) { 
                        LOG.info("Got connection {}", c);
                    }
                });
    }

    @DisplayName("Autoconfiguration will create XADataSource with provided dataSourceClassName")
    @Test
    void testAutoconfigureAgroalDataSourceWithXaDataSourceClassName() {
        runner.withConfiguration(UserConfigurations.of(NarayanaConfiguration.class))
                .withPropertyValues(
                        "narayana.logDir=ObjectStore",
                        "spring.datasource.url=jdbc:irrelevant",
                        "spring.datasource.xa.dataSourceClassName=io.agroal.test.MockXADataSource$Empty")
                .run(context -> {
                    AgroalDataSource dataSource = context.getBean(AgroalDataSource.class);

                    try (Connection c = dataSource.getConnection()) {
                        LOG.info("Got connection {}", c);
                    }
                });
    }

    @DisplayName("Autoconfiguration will create XADataSource with provided xaproperties")
    @Test
    void testAutoconfigureAgroalDataSourceWithXaProperties() {
        runner.withConfiguration(UserConfigurations.of(NarayanaConfiguration.class))
                .withPropertyValues(
                        "narayana.logDir=ObjectStore",
                        "spring.datasource.xa.dataSourceClassName=org.h2.jdbcx.JdbcDataSource",
                        "spring.datasource.xa.properties.URL=jdbc:h2:mem:test")
                .run(context -> {
                    AgroalDataSource dataSource = context.getBean(AgroalDataSource.class);

                    try (Connection c = dataSource.getConnection()) {
                        LOG.info("Got connection {}", c);
                        assertThat(c.getMetaData().getURL()).isEqualTo("jdbc:h2:mem:test");
                    }
                });
    }

    @DisplayName("Autoconfiguration will not trigger when AgroalDataSource is not on the classpath")
    @Test
    void testAutoconfigureAgroalDataSourceNotPresentOnClasspath() {
        runner
                .withClassLoader(new FilteredClassLoader(AgroalDataSource.class))
                .run(context -> assertThat(context).doesNotHaveBean(AgroalDataSourceConfiguration.class));
    }

    @DisplayName("The context will not start due to a cycle when forcing the wrong order of autoconfigurations")
    @Test
    void testAutoconfigurationWithCycle() {
        assertThatThrownBy(
                () -> runner.withConfiguration(AutoConfigurations.of(WrongOrderForcingAutoConfiguration.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("AutoConfigure cycle detected");
    }

    @AutoConfiguration(after = DataSourceAutoConfiguration.class, before = AgroalDataSourceConfiguration.class)
    private static class WrongOrderForcingAutoConfiguration {

    }
}
