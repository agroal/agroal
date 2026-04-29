// Copyright (C) 2026 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper.closed;

import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Sentinel implementation of {@link XAConnection} that throws {@link SQLException} on all operations.
 *
 * @author <a href="gegastaldi@gmail.com">George Gastaldi</a>
 */
public final class ClosedXAConnection implements XAConnection {

    public static final ClosedXAConnection INSTANCE = new ClosedXAConnection();

    // --- //

    private ClosedXAConnection() {
    }

    private static SQLException closed() {
        return new SQLException( "XAConnection is closed" );
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw closed();
    }

    @Override
    public XAResource getXAResource() throws SQLException {
        throw closed();
    }

    @Override
    public void addConnectionEventListener(ConnectionEventListener listener) {
        throw new RuntimeException( closed() );
    }

    @Override
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        throw new RuntimeException( closed() );
    }

    @Override
    public void addStatementEventListener(StatementEventListener listener) {
        throw new RuntimeException( closed() );
    }

    @Override
    public void removeStatementEventListener(StatementEventListener listener) {
        throw new RuntimeException( closed() );
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
