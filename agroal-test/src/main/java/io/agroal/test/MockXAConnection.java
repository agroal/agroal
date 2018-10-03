// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test;

import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import java.sql.Connection;
import java.sql.SQLException;

import static java.lang.System.identityHashCode;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface MockXAConnection extends XAConnection {

    @Override
    default XAResource getXAResource() throws SQLException {
        return new MockXAResource.Empty();
    }

    @Override
    default Connection getConnection() throws SQLException {
        return new MockConnection.Empty();
    }

    @Override
    default void close() throws SQLException {
    }

    @Override
    default void addConnectionEventListener(ConnectionEventListener listener) {
    }

    @Override
    default void removeConnectionEventListener(ConnectionEventListener listener) {
    }

    @Override
    default void addStatementEventListener(StatementEventListener listener) {
    }

    @Override
    default void removeStatementEventListener(StatementEventListener listener) {
    }

    // --- //

    class Empty implements MockXAConnection {

        @Override
        public String toString() {
            return "MockXAConnection@" + identityHashCode( this );
        }
    }
}
