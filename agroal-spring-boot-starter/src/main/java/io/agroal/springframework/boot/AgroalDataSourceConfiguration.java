// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.springframework.boot;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.jta.JtaTransactionManager;

import javax.sql.DataSource;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Configuration( proxyBeanMethods = false )
@ConditionalOnClass( AgroalDataSource.class )
@ConditionalOnMissingBean( DataSource.class )
@ConditionalOnProperty( name = "spring.datasource.type", havingValue = "io.agroal.springframework.AgroalDataSource", matchIfMissing = true )
public class AgroalDataSourceConfiguration {

    @Autowired( required = false )
    public JtaTransactionManager jtaPlatform;

    @Bean
    @ConfigurationProperties( prefix = "spring.datasource.agroal" )
    public AgroalDataSource dataSource(DataSourceProperties properties) {
        AgroalDataSource dataSource = properties.initializeDataSourceBuilder().type( AgroalDataSource.class ).build();
        dataSource.setName( properties.determineDatabaseName() );
        if ( jtaPlatform != null && jtaPlatform.getTransactionManager() != null && jtaPlatform.getTransactionSynchronizationRegistry() != null) {
            dataSource.setJtaTransactionIntegration( jtaPlatform );
        }
        return dataSource;
    }
}
