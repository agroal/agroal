// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.springframework.boot.health;

import io.agroal.api.AgroalDataSource;
import io.agroal.springframework.boot.AgroalDataSourceAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.autoconfigure.jdbc.DataSourceHealthContributorAutoConfiguration;
import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.util.Assert;

import java.util.Map;

@AutoConfiguration( before = DataSourceHealthContributorAutoConfiguration.class, after = AgroalDataSourceAutoConfiguration.class )
@ConditionalOnClass( { DataSourceHealthContributorAutoConfiguration.class, AgroalDataSource.class } )
@ConditionalOnBean( AgroalDataSource.class )
@ConditionalOnEnabledHealthIndicator( "db" )
public class AgroalDataSourceHealthContributorAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean( name = { "dbHealthIndicator", "dbHealthContributor" } )
    public HealthContributor dbHealthContributor( Map<String, AgroalDataSource> dataSources ) {
        return createContributor( dataSources );
    }

    private HealthContributor createContributor( Map<String, AgroalDataSource> beans ) {
        Assert.notEmpty( beans, "Beans must not be empty" );
        if ( beans.size() == 1 ) {
            return createContributor( beans.values().iterator().next() );
        }
        return CompositeHealthContributor.fromMap( beans, this::createContributor );
    }

    private HealthContributor createContributor( AgroalDataSource source ) {
        return new AgroalDataSourceHealthIndicator( source );
    }
}
