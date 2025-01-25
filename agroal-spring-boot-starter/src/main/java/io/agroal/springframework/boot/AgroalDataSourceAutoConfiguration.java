// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.springframework.boot;

import java.util.List;

import io.agroal.api.security.AgroalSecurityProvider;
import io.agroal.narayana.NarayanaTransactionIntegration;
import io.agroal.springframework.boot.jndi.AgroalDataSourceJndiBinder;
import io.agroal.springframework.boot.jndi.DefaultAgroalDataSourceJndiBinder;
import io.agroal.springframework.boot.metrics.AgroalPoolDataSourceMetadataProviderConfiguration;
import org.jboss.tm.XAResourceRecoveryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.jta.JtaTransactionManager;

import javax.sql.DataSource;

import static org.springframework.boot.jdbc.DatabaseDriver.fromJdbcUrl;
import static org.springframework.util.StringUtils.hasLength;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@AutoConfiguration( before = DataSourceAutoConfiguration.class )
@ConditionalOnClass( AgroalDataSource.class )
@ConditionalOnMissingBean( DataSource.class )
@ConditionalOnProperty( name = "spring.datasource.type", havingValue = "io.agroal.springframework.boot.AgroalDataSource", matchIfMissing = true )
@EnableConfigurationProperties( DataSourceProperties.class )
@Import( AgroalPoolDataSourceMetadataProviderConfiguration.class )
public class AgroalDataSourceAutoConfiguration {

    private final Logger logger = LoggerFactory.getLogger( AgroalDataSourceAutoConfiguration.class );

    private final JtaTransactionManager jtaPlatform;
    private final XAResourceRecoveryRegistry recoveryRegistry;

    public AgroalDataSourceAutoConfiguration(
            @Autowired( required = false ) JtaTransactionManager jtaPlatform,
            @Autowired( required = false ) XAResourceRecoveryRegistry recoveryRegistry ) {
        this.jtaPlatform = jtaPlatform;
        this.recoveryRegistry = recoveryRegistry;
    }

    @Bean
    static AgroalJdbcConnectionDetailsBeanPostProcessor agroalJdbcConnectionDetailsBeanPostProcessor(
            ObjectProvider<JdbcConnectionDetails> connectionDetailsProvider,
            ObjectProvider<JtaTransactionManager> jtaPlatformProvider ) {
        return new AgroalJdbcConnectionDetailsBeanPostProcessor( connectionDetailsProvider, jtaPlatformProvider );
    }

    @Bean
    @ConfigurationProperties( prefix = "spring.datasource.agroal" )
    @SuppressWarnings( {"HardcodedFileSeparator", "StringConcatenation"} )
    public AgroalDataSource dataSource(
            DataSourceProperties properties,
            @Value( "${spring.datasource.agroal.connectable:false}" ) boolean connectable,
            @Value( "${spring.datasource.agroal.firstResource:false}" ) boolean firstResource,
            ObjectProvider<AgroalDataSourceJndiBinder> jndiBinder,
            ObjectProvider<AgroalSecurityProvider> securityProvider,
            @Value( "${spring.datasource.agroal.credentials:#{{}}}" ) List<Object> credentials,
            @Value( "${spring.datasource.agroal.recoveryCredentials:#{{}}}" ) List<Object> recoveryCredentials ) {

        AgroalDataSource dataSource = properties.initializeDataSourceBuilder().type( AgroalDataSource.class ).build();
        if ( !hasLength( properties.getDriverClassName() ) ) {
            if ( connectable ) {
                dataSource.setDriverClassName( fromJdbcUrl( properties.determineUrl() ).getDriverClassName() );
            } else if ( hasLength( properties.getXa().getDataSourceClassName() ) ) {
                dataSource.setDriverClassName( properties.getXa().getDataSourceClassName() );
            } else {
                dataSource.setDriverClassName( fromJdbcUrl( properties.determineUrl() ).getXaDataSourceClassName() );
            }
        }
        String name = properties.determineDatabaseName();
        dataSource.setName( name );
        String jndiName = properties.getJndiName() != null ? properties.getJndiName() : "java:comp/env/jdbc/" + name;
        if ( jtaPlatform != null && jtaPlatform.getTransactionManager() != null && jtaPlatform.getTransactionSynchronizationRegistry() != null ) {
            dataSource.setJtaTransactionIntegration( new NarayanaTransactionIntegration(
                    jtaPlatform.getTransactionManager(),
                    jtaPlatform.getTransactionSynchronizationRegistry(),
                    jndiName,
                    connectable,
                    firstResource,
                    connectable ? null : recoveryRegistry )
            );
            if ( connectable && jndiBinder.getIfUnique( DefaultAgroalDataSourceJndiBinder::new ).bindToJndi( jndiName, dataSource ) ) {
                logger.info( "Bind DataSource {} as {} to JNDI registry", name, jndiName );
            }
        }
        if ( !connectable && !properties.getXa().getProperties().isEmpty() ) {
            dataSource.setXaProperties( properties.getXa().getProperties() );
        }
        securityProvider.orderedStream().forEach( dataSource::addSecurityProvider );
        credentials.forEach( dataSource::addCredential );
        recoveryCredentials.forEach( dataSource::addRecoveryCredential );
        return dataSource;
    }
}
