// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.hikari;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.AgroalDataSourceMetrics;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.security.AgroalSecurityProvider;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import static java.util.ServiceLoader.load;

/**
 * Implementation of the Agroal API wrapping the popular connection pool implementation HikariCP.
 * This implementation is not supported. Not all of the features in the Agroal API are implemented (metrics and listeners are not implemented)
 * The main purpose of this implementation is to provide a reference for some test cases and a baseline for benchmarks.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class HikariUnderTheCovers implements AgroalDataSource {

    private static final long serialVersionUID = -1655894113120947776L;

    private final AgroalDataSourceConfiguration configuration;
    private final AgroalConnectionPoolConfiguration poolConfiguration;
    private final AgroalConnectionFactoryConfiguration factoryConfiguration;

    private final HikariDataSource hikari;

    public HikariUnderTheCovers(AgroalDataSourceConfiguration dataSourceConfiguration, AgroalDataSourceListener... listeners) {
        this.configuration = dataSourceConfiguration;
        this.poolConfiguration = dataSourceConfiguration.connectionPoolConfiguration();
        this.factoryConfiguration = poolConfiguration.connectionFactoryConfiguration();
        this.hikari = new HikariDataSource( getHikariConfig( dataSourceConfiguration ) );
    }

    private HikariConfig getHikariConfig(AgroalDataSourceConfiguration dataSourceConfiguration) {
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

        for ( AgroalSecurityProvider provider : load( AgroalSecurityProvider.class, AgroalSecurityProvider.class.getClassLoader() ) ) {
            hikariConfig.setDataSourceProperties( provider.getSecurityProperties( factoryConfiguration.principal() ) );
        }
        for ( Object credential : factoryConfiguration.credentials() ) {
            for ( AgroalSecurityProvider provider : load( AgroalSecurityProvider.class, AgroalSecurityProvider.class.getClassLoader() ) ) {
                hikariConfig.setDataSourceProperties( provider.getSecurityProperties( credential ) );
            }
        }

        hikariConfig.setMaximumPoolSize( poolConfiguration.maxSize() );
        hikariConfig.setConnectionTimeout( poolConfiguration.acquisitionTimeout().toMillis() );
        hikariConfig.setMaxLifetime( poolConfiguration.maxLifetime().toMillis() );
        if ( factoryConfiguration.connectionProviderClass() != null) {
            hikariConfig.setDriverClassName( factoryConfiguration.connectionProviderClass().getName() );
        }

        return hikariConfig;
    }

    // --- //

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
