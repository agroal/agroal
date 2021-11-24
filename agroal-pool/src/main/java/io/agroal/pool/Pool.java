// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.AgroalDataSource.FlushMode;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.AgroalDataSourceMetrics;
import io.agroal.api.AgroalPoolInterceptor;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.configuration.AgroalDataSourceConfiguration.MetricsEnabledListener;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface Pool extends MetricsEnabledListener, AutoCloseable {

    void init();

    Connection getConnection() throws SQLException;

    AgroalConnectionPoolConfiguration getConfiguration();

    AgroalDataSourceMetrics getMetrics();

    AgroalDataSourceListener[] getListeners();

    List<AgroalPoolInterceptor> getPoolInterceptors();

    void setPoolInterceptors(Collection<? extends AgroalPoolInterceptor> list);

    void returnConnectionHandler(ConnectionHandler handler) throws SQLException;

    void flushPool(FlushMode mode);

    boolean isHealthy(boolean newConnection) throws SQLException;

    int defaultJdbcIsolationLevel();

    @Override
    void close();

    // --- exposed statistics //

    long activeCount();

    long maxUsedCount();

    long availableCount();

    long awaitingCount();

    void resetMaxUsedCount();
}
