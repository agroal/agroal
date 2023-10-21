// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.springframework.boot.metrics;

import io.agroal.api.AgroalDataSource;
import io.agroal.pool.DefaultMetricsRepository;
import io.agroal.springframework.boot.AgroalDataSourceConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.jdbc.DataSourceUnwrapper;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;
import java.util.Map;

@AutoConfiguration(after = {AgroalDataSourceConfiguration.class, CompositeMeterRegistryAutoConfiguration.class})
@ConditionalOnClass({AgroalDataSource.class, MeterRegistry.class})
@ConditionalOnBean({AgroalDataSource.class, MeterRegistry.class})
public class AgroalDataSourcePoolMetricsAutoConfiguration {

    @Bean
    AgroalDataSourceMeterBinder agroalDataSourceMeterBinder(Map<String, DataSource> dataSources) {
        return new AgroalDataSourceMeterBinder(dataSources);
    }

    static class AgroalDataSourceMeterBinder implements MeterBinder {

        private final Map<String, DataSource> dataSources;

        AgroalDataSourceMeterBinder(Map<String, DataSource> dataSources) {
            this.dataSources = dataSources;
        }

        @Override
        public void bindTo(MeterRegistry registry) {
            dataSources.forEach((name, dataSource) -> bindDataSourceToRegistry(name, dataSource, registry));
        }

        private void bindDataSourceToRegistry(String name, DataSource dataSource, MeterRegistry registry) {
            AgroalDataSource agroalDataSource = DataSourceUnwrapper.unwrap(dataSource, AgroalDataSource.class);
            if (agroalDataSource != null && agroalDataSource.getMetrics() instanceof DefaultMetricsRepository) {
                new AgroalDataSourcePoolMetrics(name, agroalDataSource).bindTo(registry);
            }
        }
    }
}
