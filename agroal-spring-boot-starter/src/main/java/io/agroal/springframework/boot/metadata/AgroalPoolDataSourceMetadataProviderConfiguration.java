// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.springframework.boot.metadata;

import io.agroal.api.AgroalDataSource;
import org.springframework.boot.jdbc.DataSourceUnwrapper;
import org.springframework.boot.jdbc.metadata.DataSourcePoolMetadataProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class AgroalPoolDataSourceMetadataProviderConfiguration {

    @Bean
    public DataSourcePoolMetadataProvider agroalPoolDataSourceMetadataProvider() {
        return dataSource -> {
            AgroalDataSource agroalDataSource = DataSourceUnwrapper.unwrap(dataSource, AgroalDataSource.class);
            if (agroalDataSource != null) {
                return new AgroalDataSourcePoolMetadata(agroalDataSource);
            }
            return null;
        };
    }
}
