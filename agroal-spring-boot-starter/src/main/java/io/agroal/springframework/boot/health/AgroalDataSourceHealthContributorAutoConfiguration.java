// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.springframework.boot.health;

import io.agroal.api.AgroalDataSource;
import io.agroal.springframework.boot.AgroalDataSourceAutoConfiguration;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.SimpleAutowireCandidateResolver;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.health.autoconfigure.contributor.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.jdbc.DataSourceUnwrapper;
import org.springframework.boot.jdbc.autoconfigure.health.DataSourceHealthContributorAutoConfiguration;
import org.springframework.boot.jdbc.health.DataSourceHealthIndicator;
import org.springframework.boot.jdbc.metadata.CompositeDataSourcePoolMetadataProvider;
import org.springframework.boot.jdbc.metadata.DataSourcePoolMetadata;
import org.springframework.boot.jdbc.metadata.DataSourcePoolMetadataProvider;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

import java.util.Map;
import java.util.stream.Collectors;

@AutoConfiguration( before = DataSourceHealthContributorAutoConfiguration.class, after = AgroalDataSourceAutoConfiguration.class )
@ConditionalOnClass( { DataSourceHealthContributorAutoConfiguration.class, AgroalDataSource.class, ConditionalOnEnabledHealthIndicator.class } )
@ConditionalOnBean( DataSource.class )
@ConditionalOnEnabledHealthIndicator( "db" )
public class AgroalDataSourceHealthContributorAutoConfiguration {

    private final DataSourcePoolMetadataProvider poolMetadataProvider;

    public AgroalDataSourceHealthContributorAutoConfiguration( ObjectProvider<DataSourcePoolMetadataProvider> metadataProviders ) {
        poolMetadataProvider = new CompositeDataSourcePoolMetadataProvider( metadataProviders.orderedStream().toList() );
    }

    @Bean
    @ConditionalOnMissingBean( name = { "dbHealthIndicator", "dbHealthContributor" } )
    public HealthContributor dbHealthContributor( ConfigurableListableBeanFactory beanFactory ) {
        Map<String, DataSource> dataSources = SimpleAutowireCandidateResolver.resolveAutowireCandidates( beanFactory, DataSource.class, false, true );
        Map<String, HealthContributor> healthContributors = dataSources.entrySet()
                .stream()
                .map( this::createContributor )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
        if ( healthContributors.size() == 1 ) {
            return healthContributors.values().iterator().next();
        }
        return CompositeHealthContributor.fromMap( healthContributors );
    }

    private Map.Entry<String, HealthContributor> createContributor( Map.Entry<String, DataSource> entry ) {
        AgroalDataSource agroalDataSource = DataSourceUnwrapper.unwrap( entry.getValue(), AgroalDataSource.class );
        HealthContributor hc = agroalDataSource != null ? createContributor( agroalDataSource ) : createContributor( entry.getValue() );
        return Map.entry( entry.getKey(), hc );
    }

    private HealthContributor createContributor( AgroalDataSource source ) {
        return new AgroalDataSourceHealthIndicator( source );
    }

    private HealthContributor createContributor( DataSource source ) {
        DataSourcePoolMetadata poolMetadata = poolMetadataProvider.getDataSourcePoolMetadata( source );
        String query = poolMetadata != null ? poolMetadata.getValidationQuery() : null;
        return new DataSourceHealthIndicator( source, query );
    }
}
