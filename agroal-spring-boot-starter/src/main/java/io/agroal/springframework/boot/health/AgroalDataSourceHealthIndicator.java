// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.springframework.boot.health;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import org.springframework.boot.health.contributor.AbstractHealthIndicator;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.jdbc.DatabaseDriver;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.SQLException;

import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.TransactionRequirement.OFF;
import static org.springframework.boot.jdbc.DatabaseDriver.UNKNOWN;
import static org.springframework.boot.jdbc.DatabaseDriver.fromJdbcUrl;

public class AgroalDataSourceHealthIndicator extends AbstractHealthIndicator {

    private final AgroalDataSource dataSource;
    private final Object database;

    public AgroalDataSourceHealthIndicator( AgroalDataSource dataSource ) {
        this.dataSource = dataSource;
        AgroalConnectionPoolConfiguration agroalConnectionPoolConfiguration = dataSource.getConfiguration().connectionPoolConfiguration();
        DatabaseDriver driver = fromJdbcUrl( agroalConnectionPoolConfiguration.connectionFactoryConfiguration().jdbcUrl() );
        if ( driver == UNKNOWN && agroalConnectionPoolConfiguration.transactionRequirement() == OFF ) {
            this.database = new JdbcTemplate( dataSource ).execute( this::getProduct );
        } else {
            this.database = driver;
        }
    }

    private String getProduct( Connection connection ) throws SQLException {
        return connection.getMetaData().getDatabaseProductName();
    }

    @Override
    protected void doHealthCheck( Health.Builder builder ) throws Exception {
        builder.withDetail( "provider", "Agroal" );
        builder.withDetail( "database", database );
        builder.withDetail( "validator", dataSource.getConfiguration().connectionPoolConfiguration().connectionValidator().getClass() );
        boolean healthy = dataSource.isHealthy( false );
        builder.status( (healthy) ? Status.UP : Status.DOWN );
    }
}
