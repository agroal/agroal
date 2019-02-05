// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import static java.lang.System.identityHashCode;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface MockDataSource extends DataSource {

    @Override
    default Connection getConnection() throws SQLException {
        return new MockConnection.Empty();
    }

    @Override
    default Connection getConnection(String username, String password) throws SQLException {
        return new MockConnection.Empty();
    }

    @Override
    default <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    default boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    default PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    default void setLogWriter(PrintWriter out) throws SQLException {
    }

    @Override
    default int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    default void setLoginTimeout(int seconds) throws SQLException {
    }

    @Override
    default Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    // --- //

    class Empty implements MockDataSource {

        @Override
        public String toString() {
            return "MockDataSource@" + identityHashCode( this );
        }
    }
}
