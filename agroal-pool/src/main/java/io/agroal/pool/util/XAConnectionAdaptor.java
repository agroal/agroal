// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Disguises a non-XA connection as an XAConnection. Useful to keep the same logic for pooling both XA and non-XA connections
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class XAConnectionAdaptor implements XAConnection {

    private final Connection connection;

    public XAConnectionAdaptor(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

    @Override
    public void addConnectionEventListener(ConnectionEventListener listener) {
        throw new IllegalArgumentException( "no ConnectionEventListener on non-XA connection" );
    }

    @Override
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        throw new IllegalArgumentException( "no ConnectionEventListener on non-XA connection" );
    }

    @Override
    public void addStatementEventListener(StatementEventListener listener) {
        throw new IllegalArgumentException( "no StatementEventListener on non-XA connection" );
    }

    @Override
    public void removeStatementEventListener(StatementEventListener listener) {
        throw new IllegalArgumentException( "no StatementEventListener on non-XA connection" );
    }

    @Override
    public XAResource getXAResource() throws SQLException {
        return null;
    }
}
