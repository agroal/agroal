// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.AgroalDataSourceMetrics;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class DataSource implements AgroalDataSource {

    private static final long serialVersionUID = 6485903416474487024L;

    private final AgroalDataSourceConfiguration configuration;
    private final Pool connectionPool;

    public DataSource(AgroalDataSourceConfiguration dataSourceConfiguration, AgroalDataSourceListener... listeners) {
        configuration = dataSourceConfiguration;
        if ( configuration.dataSourceImplementation() == AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL_POOLLESS ) {
            connectionPool = new Poolless( dataSourceConfiguration.connectionPoolConfiguration(), listeners );
        } else {
            connectionPool = new ConnectionPool( dataSourceConfiguration.connectionPoolConfiguration(), listeners );
        }

        dataSourceConfiguration.registerMetricsEnabledListener( connectionPool );
        connectionPool.onMetricsEnabled( dataSourceConfiguration.metricsEnabled() );
        connectionPool.init();
    }

    // --- AgroalDataSource methods //

    @Override
    public AgroalDataSourceConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public AgroalDataSourceMetrics getMetrics() {
        return connectionPool.getMetrics();
    }

    @Override
    public void flush(FlushMode mode) {
        connectionPool.flushPool(mode);
    }

    @Override
    public void close() {
        connectionPool.close();
    }

    // --- DataSource methods //

    @Override
    public Connection getConnection() throws SQLException {
        return connectionPool.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new SQLException( "username and password combination invalid on a pooled data source!" );
    }

    // --- Wrapper methods //

    @Override
    public <T> T unwrap(Class<T> target) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> target) throws SQLException {
        return false;
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
}
