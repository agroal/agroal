// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.springframework.boot.health;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.springframework.boot.jdbc.DatabaseDriver.fromJdbcUrl;

public class AgroalDataSourceHealthIndicator extends AbstractHealthIndicator {

    private final AgroalDataSource dataSource;

    public AgroalDataSourceHealthIndicator( AgroalDataSource dataSource ) {
        this.dataSource = dataSource;
    }

    @Override
    protected void doHealthCheck( Health.Builder builder ) throws Exception {
        AgroalConnectionPoolConfiguration agroalConnectionPoolConfiguration = dataSource.getConfiguration().connectionPoolConfiguration();
        builder.withDetail( "provider", "Agroal" );
        builder.withDetail( "database", fromJdbcUrl( agroalConnectionPoolConfiguration.connectionFactoryConfiguration().jdbcUrl() ) );
        builder.withDetail( "validator", agroalConnectionPoolConfiguration.connectionValidator().getClass() );
        boolean healthy = dataSource.isHealthy( false );
        builder.status( (healthy) ? Status.UP : Status.DOWN );
    }
}
