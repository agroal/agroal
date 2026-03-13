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

    private final Object databaseProductNameLock = new Object();

    private volatile String cachedDatabaseProductName;

    public AgroalDataSourceHealthIndicator( AgroalDataSource dataSource ) {
        this.dataSource = dataSource;
    }

    @Override
    protected void doHealthCheck( Health.Builder builder ) throws Exception {
        final AgroalConnectionPoolConfiguration agroalConnectionPoolConfiguration = this.dataSource.getConfiguration().connectionPoolConfiguration();
        builder.withDetail( "provider", "Agroal" );

        // Check health status first to gate metadata lookup
        final boolean healthy = this.dataSource.isHealthy( false );

        // Try to determine database from JDBC URL first
        final String jdbcUrl = agroalConnectionPoolConfiguration.connectionFactoryConfiguration().jdbcUrl();
        final DatabaseDriver database = fromJdbcUrl( jdbcUrl );

        // If JDBC URL is not available or returns UNKNOWN, fallback to connection metadata
        // Only attempt metadata lookup if health is UP to avoid unnecessary overhead during outages
        if ( database == DatabaseDriver.UNKNOWN ) {
            final String productName = this.getDatabaseProductName( healthy );
            builder.withDetail( "database", productName != null ? productName : database );
        } else {
            builder.withDetail( "database", database );
        }

        builder.withDetail( "validator", agroalConnectionPoolConfiguration.connectionValidator().getClass() );
        builder.status( (healthy) ? Status.UP : Status.DOWN );
    }

    /**
     * Get the database product name from connection metadata.
     * This is used as a fallback when JDBC URL is not available or doesn't contain database info.
     * The result is cached after the first successful call.
     * 
     * Metadata lookup is only attempted when the pool health is UP to avoid unnecessary
     * connection overhead during outages. On lookup failure, no failure state is cached,
     * allowing automatic retry on the next health check when pool health recovers.
     *
     * @param isHealthy whether the pool is currently healthy
     * @return the database product name, or null if it cannot be determined or pool is unhealthy
     */
    private String getDatabaseProductName( final boolean isHealthy ) {
        String productName = this.cachedDatabaseProductName;
        if ( productName != null || !isHealthy ) {
            return productName;
        }

        synchronized ( this.databaseProductNameLock ) {
            productName = this.cachedDatabaseProductName;
            if ( productName == null ) {
                try ( final Connection connection = this.dataSource.getConnection() ) {
                    productName = connection.getMetaData().getDatabaseProductName();
                    this.cachedDatabaseProductName = productName;
                } catch ( final Exception e ) {
                    // Do not cache failure state; retry will occur naturally on next UP health check
                }
            }
        }
        return productName;
    }
}
