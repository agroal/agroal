// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.hikari;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.AgroalDataSourceMetrics;
import io.agroal.api.AgroalPoolInterceptor;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.security.AgroalSecurityProvider;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Implementation of the Agroal API wrapping the popular connection pool implementation HikariCP.
 * This implementation is not supported. Not all features in the Agroal API are implemented (metrics and listeners are not implemented)
 * The main purpose of this implementation is to provide a reference for some test cases and a baseline for benchmarks.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class HikariUnderTheCovers implements AgroalDataSource {

    private static final long serialVersionUID = -1655894113120947776L;

    private final AgroalDataSourceConfiguration configuration;
    private final AgroalConnectionPoolConfiguration poolConfiguration;
    private final AgroalConnectionFactoryConfiguration factoryConfiguration;

    @SuppressWarnings( "NonSerializableFieldInSerializableClass" )
    private final HikariDataSource hikari;
    private final AgroalDataSourceListener[] agroalListeners;

    public HikariUnderTheCovers(AgroalDataSourceConfiguration dataSourceConfiguration, AgroalDataSourceListener... listeners) {
        configuration = dataSourceConfiguration;
        poolConfiguration = dataSourceConfiguration.connectionPoolConfiguration();
        factoryConfiguration = poolConfiguration.connectionFactoryConfiguration();
        agroalListeners = listeners;
        hikari = new HikariDataSource( getHikariConfig() );
    }

    @SuppressWarnings( "StringConcatenation" )
    private HikariConfig getHikariConfig() {
        HikariConfig hikariConfig = new HikariConfig();

        // hikariConfig.setDataSourceJNDI( dataSourceConfiguration.jndiName() );
        hikariConfig.setLeakDetectionThreshold( poolConfiguration.leakTimeout().toMillis() );
        hikariConfig.setIdleTimeout( poolConfiguration.reapTimeout().toMillis() );
        hikariConfig.setValidationTimeout( poolConfiguration.validationTimeout().toMillis() );

        if ( factoryConfiguration.jdbcTransactionIsolation().isDefined() ) {
            hikariConfig.setTransactionIsolation( "TRANSACTION_" + factoryConfiguration.jdbcTransactionIsolation() );
        }

        hikariConfig.setJdbcUrl( factoryConfiguration.jdbcUrl() );
        hikariConfig.setAutoCommit( factoryConfiguration.autoCommit() );
        hikariConfig.setConnectionInitSql( factoryConfiguration.initialSql() );

        for ( AgroalSecurityProvider provider : configuration.connectionPoolConfiguration().connectionFactoryConfiguration().securityProviders() ) {
            Properties properties = provider.getSecurityProperties( factoryConfiguration.principal() );
            if ( properties != null ) {
                hikariConfig.setDataSourceProperties( properties );
            }
        }
        for ( Object credential : factoryConfiguration.credentials() ) {
            for ( AgroalSecurityProvider provider : configuration.connectionPoolConfiguration().connectionFactoryConfiguration().securityProviders() ) {
                Properties properties = provider.getSecurityProperties( credential );
                if ( properties != null ) {
                    hikariConfig.setDataSourceProperties( properties );
                }
            }
        }

        hikariConfig.setMaximumPoolSize( poolConfiguration.maxSize() );
        hikariConfig.setConnectionTimeout( poolConfiguration.acquisitionTimeout().toMillis() );
        hikariConfig.setMaxLifetime( poolConfiguration.maxLifetime().toMillis() );

        if ( factoryConfiguration.connectionProviderClass() != null ) {
            hikariConfig.setDriverClassName( factoryConfiguration.connectionProviderClass().getName() );
        }

        return hikariConfig;
    }

    // --- //

    @Override
    public void setPoolInterceptors(Collection<? extends AgroalPoolInterceptor> interceptors) {
        throw new UnsupportedOperationException( "Hikari pool does not support interceptors" );
    }

    @Override
    public List<AgroalPoolInterceptor> getPoolInterceptors() {
        return Collections.emptyList();
    }

    @Override
    public String getUrl() {
        return factoryConfiguration.jdbcUrl();
    }

    @Override
    public AgroalDataSourceConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public AgroalDataSourceMetrics getMetrics() {
        throw new UnsupportedOperationException( "Hikari pool does not expose metrics" );
    }

    @Override
    public void flush(FlushMode mode) {
        hikari.getHikariPoolMXBean().softEvictConnections();
    }

    @Override
    public boolean isHealthy(boolean newConnection) throws SQLException {
        if ( newConnection ) {
            for ( AgroalDataSourceListener listener : agroalListeners ) {
                listener.onInfo( "Health check in Hikari performed in pooled connection instead of new connection" );
            }
        }
        try ( Connection connection = getConnection() ) {
            return configuration.connectionPoolConfiguration().connectionValidator().isValid( connection );
        }
    }

    @Override
    public void close() {
        hikari.close();
    }

    // --- //

    @Override
    public Connection getConnection() throws SQLException {
        return hikari.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return hikari.getConnection( username, password );
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return hikari.unwrap( iface );
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return hikari.isWrapperFor( iface );
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return hikari.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        hikari.setLogWriter( out );
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return hikari.getLoginTimeout();
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        hikari.setLoginTimeout( seconds );
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return hikari.getParentLogger();
    }
}
