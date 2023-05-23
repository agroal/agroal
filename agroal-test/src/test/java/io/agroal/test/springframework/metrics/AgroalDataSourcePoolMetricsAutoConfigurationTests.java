// Copyright (C) 2023 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.springframework.metrics;

import io.agroal.springframework.boot.AgroalDataSource;
import io.agroal.springframework.boot.AgroalDataSourceConfiguration;
import io.agroal.springframework.boot.metrics.AgroalDataSourcePoolMetricsAutoConfiguration;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.sql.Connection;
import java.util.Map;

import static io.agroal.test.AgroalTestGroup.SPRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag( SPRING )
class AgroalDataSourcePoolMetricsAutoConfigurationTests {

    private final static Logger LOG = LoggerFactory.getLogger(AgroalDataSourcePoolMetricsAutoConfigurationTests.class);

    private final Class<?>[] autoconfigurationsInWrongOrder = new Class<?>[]{
            AgroalDataSourcePoolMetricsAutoConfiguration.class,
            AgroalDataSourceConfiguration.class,
            CompositeMeterRegistryAutoConfiguration.class,
            SimpleMetricsExportAutoConfiguration.class,
            MetricsAutoConfiguration.class
    };

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(autoconfigurationsInWrongOrder));

    @DisplayName("Autoconfigurations will trigger in the correct order and the beans will be registered and created")
    @Test
    void testAutoconfigureAgroalDataSourceMeterBinder() {
        runner.withPropertyValues("spring.datasource.agroal.metrics=true")
                .run(context -> {
                    assertThat(context).hasSingleBean(MeterBinder.class);
                    assertThat(context).hasSingleBean(AgroalDataSource.class);

                    MeterBinder meterBinder = context.getBean(MeterBinder.class);
                    AgroalDataSource dataSource = context.getBean(AgroalDataSource.class);

                    assertThat(meterBinder).hasFieldOrPropertyWithValue("dataSources", Map.of("dataSource", dataSource));
                    MeterRegistry registry = context.getBean(MeterRegistry.class);
                    Gauge gauge = registry.get("agroal.connections.creation.count").gauge();
                    assertThat(gauge.value()).isEqualTo(0.0);

                    try (Connection c = dataSource.getConnection()) {
                        LOG.info("Got connection {}", c);
                        assertThat(gauge.value()).isEqualTo(1.0);
                    }
                });
    }

    @DisplayName("AgroalDataSourcePoolMetricsAutoConfiguration will not trigger without micrometer on the classpath")
    @Test
    void testMicrometerNotPresentOnClasspath() {
        runner
                .withClassLoader(new FilteredClassLoader(MeterRegistry.class))
                .run(context -> assertThat(context).doesNotHaveBean(AgroalDataSourcePoolMetricsAutoConfiguration.class));
    }

    @DisplayName("The context will not start due to a cycle when forcing the wrong order of autoconfigurations")
    @Test
    void testAutoconfigurationWithCycle() {
        assertThatThrownBy(
                () -> runner.withConfiguration(AutoConfigurations.of(WrongOrderForcingAutoConfiguration.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("AutoConfigure cycle detected");
    }

    @AutoConfiguration(
            after = AgroalDataSourcePoolMetricsAutoConfiguration.class,
            before = {
                    AgroalDataSourceConfiguration.class,
                    MetricsAutoConfiguration.class,
                    SimpleMetricsExportAutoConfiguration.class,
                    CompositeMeterRegistryAutoConfiguration.class
            })
    private static class WrongOrderForcingAutoConfiguration {

    }
}
