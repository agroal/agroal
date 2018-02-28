// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.AgroalDataSourceMetrics;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.AgroalDataSourceConfiguration.MetricsEnabledListener;
import io.agroal.pool.MetricsRepository.EmptyMetricsRepository;
import io.agroal.pool.util.UncheckedArrayList;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class DataSource implements AgroalDataSource, MetricsEnabledListener {

    private static final long serialVersionUID = 6485903416474487024L;

    private final AgroalDataSourceConfiguration configuration;
    private final List<AgroalDataSourceListener> listenerList;
    private final ConnectionPool connectionPool;
    private MetricsRepository dataSourceMetrics;

    @SuppressWarnings( "ThisEscapedInObjectConstruction" )
    public DataSource(AgroalDataSourceConfiguration dataSourceConfiguration, AgroalDataSourceListener... listeners) {
        configuration = dataSourceConfiguration;
        connectionPool = new ConnectionPool( dataSourceConfiguration.connectionPoolConfiguration(), this );
        dataSourceConfiguration.registerMetricsEnabledListener( this );
        onMetricsEnabled( dataSourceConfiguration.metricsEnabled() );

        if ( listeners.length == 0 ) {
            listenerList = Collections.emptyList();
        } else {
            listenerList = new UncheckedArrayList<>( AgroalDataSourceListener.class, listeners );
        }

        connectionPool.init();
    }

    public Collection<AgroalDataSourceListener> listenerList() {
        return listenerList;
    }

    public MetricsRepository metricsRepository() {
        return dataSourceMetrics;
    }

    @Override
    public void onMetricsEnabled(boolean metricsEnabled) {
        dataSourceMetrics = metricsEnabled ? new DefaultMetricsRepository( connectionPool ) : new EmptyMetricsRepository();
    }

    // --- AgroalDataSource methods //

    @Override
    public AgroalDataSourceConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public AgroalDataSourceMetrics getMetrics() {
        return dataSourceMetrics;
    }

    @Deprecated
    public void addListener(AgroalDataSourceListener listener) {
        throw new UnsupportedOperationException( "Deprecated. Add listeners using the constructor or AgroalDataSource factory methods" );
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
