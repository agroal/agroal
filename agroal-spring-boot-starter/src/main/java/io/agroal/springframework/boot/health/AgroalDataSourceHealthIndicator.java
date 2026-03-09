// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.springframework.boot.health;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import org.springframework.boot.health.contributor.AbstractHealthIndicator;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.jdbc.DatabaseDriver;

import java.sql.Connection;

import static org.springframework.boot.jdbc.DatabaseDriver.fromJdbcUrl;

public class AgroalDataSourceHealthIndicator extends AbstractHealthIndicator {

    private final AgroalDataSource dataSource;

    private volatile String cachedDatabaseProductName;

    public AgroalDataSourceHealthIndicator( AgroalDataSource dataSource ) {
        this.dataSource = dataSource;
    }

    @Override
    protected void doHealthCheck( Health.Builder builder ) throws Exception {
        final AgroalConnectionPoolConfiguration agroalConnectionPoolConfiguration = this.dataSource.getConfiguration().connectionPoolConfiguration();
        builder.withDetail( "provider", "Agroal" );

        // Try to determine database from JDBC URL first
        final String jdbcUrl = agroalConnectionPoolConfiguration.connectionFactoryConfiguration().jdbcUrl();
        final DatabaseDriver database = fromJdbcUrl( jdbcUrl );

        // If JDBC URL is not available or returns UNKNOWN, fallback to connection metadata
        if ( database == DatabaseDriver.UNKNOWN ) {
            final String productName = this.getDatabaseProductName();
            builder.withDetail( "database", productName != null ? productName : database );
        } else {
            builder.withDetail( "database", database );
        }

        builder.withDetail( "validator", agroalConnectionPoolConfiguration.connectionValidator().getClass() );
        final boolean healthy = this.dataSource.isHealthy( false );
        builder.status( (healthy) ? Status.UP : Status.DOWN );
    }

    /**
     * Get the database product name from connection metadata.
     * This is used as a fallback when JDBC URL is not available or doesn't contain database info.
     * The result is cached after the first call to avoid creating connections on every health check.
     *
     * @return the database product name, or null if it cannot be determined
     */
    private String getDatabaseProductName() {
        if (this.cachedDatabaseProductName == null ) {
            synchronized ( this ) {
                if (this.cachedDatabaseProductName == null ) {
                    try (final Connection connection = this.dataSource.getConnection() ) {
                        this.cachedDatabaseProductName = connection.getMetaData().getDatabaseProductName();
                    } catch ( final Exception e ) {
                        // If we can't get metadata, cache empty string to avoid repeated attempts
                        this.cachedDatabaseProductName = "";
                    }
                }
            }
        }
        // Return null if we cached an empty string (failed to get product name)
        return this.cachedDatabaseProductName.isEmpty() ? null : this.cachedDatabaseProductName;
    }
}
