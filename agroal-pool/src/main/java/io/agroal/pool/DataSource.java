// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.AgroalDataSourceMetrics;
import io.agroal.api.AgroalPoolInterceptor;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation;
import io.agroal.api.configuration.supplier.AgroalConnectionFactoryConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalConnectionPoolConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.agroal.pool.util.ReloadDataSourceUtil;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static java.util.Collections.emptyList;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class DataSource implements AgroalDataSource {

    private static final long serialVersionUID = 6485903416474487024L;

    private final AtomicReference<AgroalDataSourceConfiguration> configuration  = new AtomicReference<>();
    private final AtomicReference<Pool> connectionPool  = new AtomicReference<>();
    private final AtomicReference<AgroalDataSourceListener[]> listeners = new AtomicReference<>();

    public DataSource(AgroalDataSourceConfiguration dataSourceConfiguration, AgroalDataSourceListener... listeners) {
        configuration.set(dataSourceConfiguration);
        if ( configuration.get().dataSourceImplementation() == AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL_POOLLESS ) {
            connectionPool.set(new Poolless( dataSourceConfiguration.connectionPoolConfiguration(), listeners ));
        } else {
            connectionPool.set(new ConnectionPool( dataSourceConfiguration.connectionPoolConfiguration(), listeners ));
        }

        this.listeners.set(listeners);

        dataSourceConfiguration.registerMetricsEnabledListener( connectionPool.get());
        connectionPool.get().onMetricsEnabled( dataSourceConfiguration.metricsEnabled() );
        connectionPool.get().init();
    }

    // --- AgroalDataSource methods //

    @Override
    public void setPoolInterceptors(Collection<? extends AgroalPoolInterceptor> interceptors) {
        connectionPool.get().setPoolInterceptors( interceptors == null ? emptyList() : interceptors );
    }

    @Override
    public List<AgroalPoolInterceptor> getPoolInterceptors() {
        return connectionPool.get().getPoolInterceptors();
    }

    @Override
    public AgroalDataSourceConfiguration getConfiguration() {
        return configuration.get();
    }

    @Override
    public AgroalDataSourceMetrics getMetrics() {
        return connectionPool.get().getMetrics();
    }

    @Override
    public void flush(FlushMode mode) {
        connectionPool.get().flushPool( mode );
    }

    @Override
    public boolean isHealthy(boolean newConnection) throws SQLException {
        return connectionPool.get().isHealthy( newConnection );
    }

    @Override
    public void close() {
        connectionPool.get().close();
    }

    // --- DataSource methods //

    @Override
    public Connection getConnection() throws SQLException {
        return connectionPool.get().getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new SQLException( "username and password combination invalid on a pooled data source!" );
    }

    // --- Wrapper methods //

    @Override
    public <T> T unwrap(Class<T> target) throws SQLException {
        return target.cast( this );
    }

    @Override
    public boolean isWrapperFor(Class<?> target) throws SQLException {
        return target.isInstance( this );
    }

    // --- CommonDataSource methods //

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        // no-op
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        // no-op
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException( "Not Supported" );
    }

    // --- DataSource method //
    /**
     * Reload current DataSource
     * @param properties - key-value structure. Available keys: ReloadDataSourceUtil.AGROAL_JDBC_URL, ReloadDataSourceUtil.AGROAL_USERNAME, ReloadDataSourceUtil.AGROAL_PASSWORD
     * @param flushMode - flush mode
     */
    public synchronized void reloadDataSourceWithNewCredentials(Properties properties, FlushMode flushMode) {
        flush(flushMode);
        this.connectionPool.get().close();

        AgroalConnectionPoolConfigurationSupplier agroalConnectionPoolConfigurationSupplier = new AgroalConnectionPoolConfigurationSupplier(getConfiguration().connectionPoolConfiguration());
        AgroalConnectionFactoryConfigurationSupplier agroalConnFactConfSupplier = new AgroalConnectionFactoryConfigurationSupplier();

        AgroalConnectionFactoryConfiguration configuration = getConfiguration().connectionPoolConfiguration().connectionFactoryConfiguration();
        agroalConnFactConfSupplier.autoCommit(configuration.autoCommit());
        agroalConnFactConfSupplier.trackJdbcResources(configuration.trackJdbcResources());
        agroalConnFactConfSupplier.loginTimeout(configuration.loginTimeout());
        if (properties.containsKey(ReloadDataSourceUtil.AGROAL_JDBC_URL)) {
            agroalConnFactConfSupplier.jdbcUrl(properties.getProperty(ReloadDataSourceUtil.AGROAL_JDBC_URL));
        } else {
            agroalConnFactConfSupplier.jdbcUrl(configuration.jdbcUrl());
        }
        agroalConnFactConfSupplier.initialSql(configuration.initialSql());
        agroalConnFactConfSupplier.connectionProviderClass(configuration.connectionProviderClass());
        agroalConnFactConfSupplier.jdbcTransactionIsolation(AgroalConnectionFactoryConfiguration.TransactionIsolation.fromLevel(configuration.jdbcTransactionIsolation().level()));

        if (properties.containsKey(ReloadDataSourceUtil.AGROAL_USERNAME)) {
            NamePrincipal namePrincipal = new NamePrincipal(properties.getProperty(ReloadDataSourceUtil.AGROAL_USERNAME));

            agroalConnFactConfSupplier.principal(namePrincipal);
            agroalConnFactConfSupplier.recoveryPrincipal(namePrincipal);
        } else {
            agroalConnFactConfSupplier.principal(configuration.principal());
            agroalConnFactConfSupplier.recoveryPrincipal(configuration.recoveryPrincipal());
        }

        if (properties.containsKey(ReloadDataSourceUtil.AGROAL_PASSWORD)) {
            SimplePassword simplePassword = new SimplePassword(properties.getProperty(ReloadDataSourceUtil.AGROAL_PASSWORD));

            agroalConnFactConfSupplier.credential(simplePassword);
            agroalConnFactConfSupplier.recoveryCredential(simplePassword);
        } else {
            agroalConnFactConfSupplier.credential(configuration.credentials());
            agroalConnFactConfSupplier.recoveryCredential(configuration.recoveryCredentials());
        }

        configuration.jdbcProperties().forEach((k, v) -> agroalConnFactConfSupplier.jdbcProperty((String)k, (String)v));

        agroalConnectionPoolConfigurationSupplier.connectionFactoryConfiguration(agroalConnFactConfSupplier);

        AgroalDataSourceConfigurationSupplier agroalDataSourceConfigurationSupplier = new AgroalDataSourceConfigurationSupplier();
        agroalDataSourceConfigurationSupplier.connectionPoolConfiguration(agroalConnectionPoolConfigurationSupplier);
        AgroalDataSourceConfiguration dataSourceConfiguration = agroalDataSourceConfigurationSupplier.get();

        this.configuration.set(dataSourceConfiguration);
        if (this.configuration.get().dataSourceImplementation() == DataSourceImplementation.AGROAL_POOLLESS) {
            this.connectionPool.set(new Poolless(dataSourceConfiguration.connectionPoolConfiguration(), listeners.get()));
        } else {
            this.connectionPool.set(new ConnectionPool(dataSourceConfiguration.connectionPoolConfiguration(), listeners.get()));
        }

        dataSourceConfiguration.registerMetricsEnabledListener(this.connectionPool.get());
        this.connectionPool.get().onMetricsEnabled(dataSourceConfiguration.metricsEnabled());
        this.connectionPool.get().init();
    }
}
