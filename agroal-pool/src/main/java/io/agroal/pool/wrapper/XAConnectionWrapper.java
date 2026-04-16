// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper;

import io.agroal.pool.ConnectionHandler;
import io.agroal.pool.util.AutoCloseableElement;

import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class XAConnectionWrapper extends AutoCloseableElement<XAConnectionWrapper> implements XAConnection {

    private static final String CLOSED_HANDLER_STRING = XAConnectionWrapper.class.getSimpleName() + ".CLOSED_XA_CONNECTION";

    private static final XAConnection CLOSED_XA_CONNECTION = new ClosedXAConnection();

    // --- //

    // Collection of XAResources to close them on close(). If null Statements are not tracked.
    private final AutoCloseableElement<XAResourceWrapper> trackedXAResources;

    private final ConnectionHandler handler;

    private XAConnection wrappedXAConnection;

    public XAConnectionWrapper(ConnectionHandler connectionHandler, XAConnection xaConnection, boolean trackResources) {
        super( null );
        handler = connectionHandler;
        wrappedXAConnection = xaConnection;
        trackedXAResources = trackResources ? newHead() : null;
    }

    private XAResource trackXAResource(XAResource resource) {
        if ( trackedXAResources != null && resource != null ) {
            return new XAResourceWrapper( handler, resource, trackedXAResources );
        }
        return resource;
    }
    
    @Override
    protected boolean internalClosed() {
        return wrappedXAConnection == CLOSED_XA_CONNECTION;
    }

    // --- //

    @Override
    public boolean isClosed() throws SQLException {
        return internalClosed();
    }

    @Override
    public void close() throws SQLException {
        handler.traceConnectionOperation( "close()" );
        if ( wrappedXAConnection != CLOSED_XA_CONNECTION ) {
            wrappedXAConnection = CLOSED_XA_CONNECTION;

            if ( trackedXAResources != null ) {
                trackedXAResources.closeAllAutocloseableElements();
            }
            handler.transactionEnd();
        }
    }

    @Override
    public XAResource getXAResource() throws SQLException {
        try {
            handler.traceConnectionOperation( "getXAResource()" );
            handler.verifyEnlistment();
            return trackXAResource( handler.getXaResource() );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            handler.traceConnectionOperation( "getConnection()" );
            handler.verifyEnlistment();
            // this is used for recovery. set detached true and holdable false
            return new ConnectionWrapper( handler, trackedXAResources != null, true, false );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void addConnectionEventListener(ConnectionEventListener listener) {
        handler.traceConnectionOperation( "addConnectionEventListener()" );
        wrappedXAConnection.addConnectionEventListener( listener );
    }

    @Override
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        handler.traceConnectionOperation( "removeConnectionEventListener()" );
        wrappedXAConnection.removeConnectionEventListener( listener );
    }

    @Override
    public void addStatementEventListener(StatementEventListener listener) {
        handler.traceConnectionOperation( "addStatementEventListener()" );
        wrappedXAConnection.addStatementEventListener( listener );
    }

    @Override
    public void removeStatementEventListener(StatementEventListener listener) {
        handler.traceConnectionOperation( "removeStatementEventListener()" );
        wrappedXAConnection.removeStatementEventListener( listener );
    }

    private static final class ClosedXAConnection implements XAConnection {

        @Override public void close() { /* no-op */ }
        @Override public Connection getConnection() throws SQLException { throw new SQLException( "XAConnection is closed" ); }
        @Override public XAResource getXAResource() throws SQLException { throw new SQLException( "XAConnection is closed" ); }
        @Override public void addConnectionEventListener(ConnectionEventListener listener) { /* no-op */ }
        @Override public void removeConnectionEventListener(ConnectionEventListener listener) { /* no-op */ }
        @Override public void addStatementEventListener(StatementEventListener listener) { /* no-op */ }
        @Override public void removeStatementEventListener(StatementEventListener listener) { /* no-op */ }

        @Override
        public String toString() {
            return CLOSED_HANDLER_STRING;
        }
    }

}
