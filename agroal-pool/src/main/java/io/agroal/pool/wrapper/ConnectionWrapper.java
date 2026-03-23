// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper;

import io.agroal.pool.ConnectionHandler;
import io.agroal.pool.util.AutoCloseableElement;

import java.lang.reflect.InvocationHandler;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static java.lang.reflect.Proxy.newProxyInstance;
import static java.sql.ClientInfoStatus.REASON_UNKNOWN;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public final class ConnectionWrapper extends AutoCloseableElement<ConnectionWrapper> implements Connection {

    private static final String CLOSED_HANDLER_STRING = ConnectionWrapper.class.getSimpleName() + ".CLOSED_CONNECTION";

    private static final InvocationHandler CLOSED_HANDLER = (proxy, method, args) -> {
        switch ( method.getName() ) {
            case "abort":
            case "close":
                return Void.TYPE;
            case "isClosed":
                return Boolean.TRUE;
            case "isValid":
                return Boolean.FALSE;
            case "toString":
                return CLOSED_HANDLER_STRING;
            default:
                throw new SQLException( "Connection is closed" );
        }
    };

    private static final Connection CLOSED_CONNECTION = (Connection) newProxyInstance( Connection.class.getClassLoader(), new Class[]{Connection.class}, CLOSED_HANDLER );

    // --- //

    // Connection.close() does not return the connection to the pool.
    private final boolean detachedState;

    // tracks the current holdability state of this connection
    private boolean holdState;

    // Collection of Statements to close them on close(). If null Statements are not tracked.
    private final AutoCloseableElement<StatementWrapper> trackedStatements;
    private int leakedStatements, leakedResultSets;

    private final ConnectionHandler handler;

    private volatile Connection wrappedConnection;

    public ConnectionWrapper(ConnectionHandler connectionHandler, boolean trackResources, boolean defaultHold) {
        this( connectionHandler, trackResources, false, defaultHold );
    }

    public ConnectionWrapper(ConnectionHandler connectionHandler, boolean trackResources, boolean detached, boolean defaultHold) {
        this( connectionHandler, trackResources, null, detached, defaultHold );
    }

    public ConnectionWrapper(ConnectionHandler connectionHandler, boolean trackResources, AutoCloseableElement<ConnectionWrapper> head, boolean defaultHold) {
        this( connectionHandler, trackResources, head, false, defaultHold );
    }

    private ConnectionWrapper(ConnectionHandler connectionHandler, boolean trackResources, AutoCloseableElement<ConnectionWrapper> head, boolean detached, boolean defaultHold) {
        super( head );
        handler = connectionHandler;
        wrappedConnection = connectionHandler.rawConnection();
        trackedStatements = trackResources ? newHead() : null;
        detachedState = detached;
        holdState = defaultHold;
    }

    public void verifyEnlistment() throws SQLException {
        handler.verifyEnlistment();
    }

    /**
     * Takes a snapshot of the volatile wrappedConnection field and checks if the connection is closed.
     * Using a local variable prevents the race between verifyEnlistment() and the actual JDBC call,
     * where the transaction reaper thread could swap wrappedConnection to CLOSED_CONNECTION in between.
     */
    private Connection getWrappedConnection() throws SQLException {
        Connection c = wrappedConnection;
        if ( c == CLOSED_CONNECTION ) {
            throw new SQLException( "Connection is closed" );
        }
        return c;
    }

    public ConnectionHandler getHandler() {
        return handler;
    }

    public boolean isDetached() {
        return detachedState;
    }

    // --- //

    private Statement trackStatement(Statement statement) {
        return trackStatement( statement, holdState );
    }

    private Statement trackStatement(Statement statement, boolean holdability) {
        if ( trackedStatements != null && statement != null ) {
            return new StatementWrapper( this, statement, true, trackedStatements, holdability );
        }
        return statement;
    }

    private CallableStatement trackCallableStatement(CallableStatement statement) {
        return new CallableStatementWrapper( this, statement, true, trackedStatements, holdState );
    }

    private CallableStatement trackCallableStatement(CallableStatement statement, boolean holdability) {
        if ( trackedStatements != null && statement != null ) {
            return new CallableStatementWrapper( this, statement, true, trackedStatements, holdability );
        }
        return statement;
    }

    private PreparedStatement trackPreparedStatement(PreparedStatement statement) {
        return trackPreparedStatement( statement, holdState );
    }

    private PreparedStatement trackPreparedStatement(PreparedStatement statement, boolean holdability) {
        if ( trackedStatements != null && statement != null ) {
            return new PreparedStatementWrapper( this, statement, true, trackedStatements, holdability );
        }
        return statement;
    }

    public void closeNotHeldTrackedStatements() {
        if ( trackedStatements != null ) {
            addLeakedStatements( trackedStatements.closeNotHeldAutocloseableElements() );
        }
    }

    public boolean hasTrackedStatements() throws SQLException {
        return trackedStatements != null && trackedStatements.isElementListEmpty();
    }

    // --- //

    @Override
    public void close() throws SQLException {
        handler.traceConnectionOperation( "close()" );
        Connection c = wrappedConnection;
        if ( c != CLOSED_CONNECTION ) {
            wrappedConnection = CLOSED_CONNECTION;
            pruneClosed();
            if ( trackedStatements != null ) {
                addLeakedStatements( trackedStatements.closeAllAutocloseableElements() );
            }
            handler.onConnectionWrapperClose( this );
        }
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        handler.traceConnectionOperation( "abort()" );
        try {
            Connection c = wrappedConnection;
            wrappedConnection = CLOSED_CONNECTION;
            c.abort( executor );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        handler.traceConnectionOperation( "setAutoCommit(boolean)" );
        if ( autoCommit && handler.isEnlisted() ) {
            handler.setFlushOnly();
            throw new SQLException( "Trying to set autocommit in connection taking part of transaction" );
        }
        try {
            Connection c = getWrappedConnection();
            verifyEnlistment();
            if ( c.getAutoCommit() != autoCommit ) {
                handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.AUTOCOMMIT );
                c.setAutoCommit( autoCommit );
            }
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        try {
            handler.traceConnectionOperation( "getAutoCommit()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.getAutoCommit();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void commit() throws SQLException {
        handler.traceConnectionOperation( "commit()" );
        if ( handler.isEnlisted() ) {
            handler.setFlushOnly();
            throw new SQLException( "Attempting to commit while taking part in a transaction" );
        }
        try {
            getWrappedConnection().commit();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void rollback() throws SQLException {
        handler.traceConnectionOperation( "rollback()" );
        if ( handler.isEnlisted() ) {
            handler.setFlushOnly();
            throw new SQLException( "Attempting to rollback while enlisted in a transaction" );
        }
        try {
            Connection c = getWrappedConnection();
            verifyEnlistment();
            c.rollback();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        handler.traceConnectionOperation( "rollback(Savepoint)" );
        if ( handler.isEnlisted() ) {
            handler.setFlushOnly();
            throw new SQLException( "Attempting to rollback while enlisted in a transaction" );
        }
        try {
            Connection c = getWrappedConnection();
            verifyEnlistment();
            c.rollback( savepoint );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    // --- //

    @Override
    public void clearWarnings() throws SQLException {
        try {
            handler.traceConnectionOperation( "clearWarnings()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            c.clearWarnings();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Clob createClob() throws SQLException {
        try {
            handler.traceConnectionOperation( "createClob()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.createClob();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Blob createBlob() throws SQLException {
        try {
            handler.traceConnectionOperation( "createBlob()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.createBlob();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public NClob createNClob() throws SQLException {
        try {
            handler.traceConnectionOperation( "createNClob()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.createNClob();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        try {
            handler.traceConnectionOperation( "createSQLXML()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.createSQLXML();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        try {
            handler.traceConnectionOperation( "createArrayOf(String, Object[])" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.createArrayOf( typeName, elements );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        try {
            handler.traceConnectionOperation( "createStatement()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return trackStatement( c.createStatement() );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            handler.traceConnectionOperation( "createStatement(int, int)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return trackStatement( c.createStatement( resultSetType, resultSetConcurrency ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        try {
            handler.traceConnectionOperation( "createStatement(int, int, int)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return trackStatement( c.createStatement( resultSetType, resultSetConcurrency, resultSetHoldability ), resultSetHoldability == HOLD_CURSORS_OVER_COMMIT );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        try {
            handler.traceConnectionOperation( "createStruct(String, Object[])" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.createStruct( typeName, attributes );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String getCatalog() throws SQLException {
        try {
            handler.traceConnectionOperation( "getCatalog()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.getCatalog();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        try {
            handler.traceConnectionOperation( "setCatalog(String)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.CATALOG );
            c.setCatalog( catalog );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        try {
            handler.traceConnectionOperation( "getHoldability()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.getHoldability();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        try {
            handler.traceConnectionOperation( "setHoldability(int)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.HOLDABILITY );
            holdState = ( holdability == HOLD_CURSORS_OVER_COMMIT );
            c.setHoldability( holdability );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        try {
            handler.traceConnectionOperation( "getClientInfo()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.getClientInfo();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            handler.traceConnectionOperation( "setClientInfo(Properties)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            c.setClientInfo( properties );
        } catch ( SQLClientInfoException sce ) {
            handler.setFlushOnly( sce );
            throw sce;
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw new SQLClientInfoException( properties == null ? emptyMap() : properties.stringPropertyNames().stream().collect( toMap( p -> p, p -> REASON_UNKNOWN ) ) );
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        try {
            handler.traceConnectionOperation( "getClientInfo(String)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.getClientInfo( name );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        try {
            handler.traceConnectionOperation( "getMetaData()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.getMetaData();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        try {
            handler.traceConnectionOperation( "getNetworkTimeout()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.getNetworkTimeout();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String getSchema() throws SQLException {
        try {
            handler.traceConnectionOperation( "getSchema()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.getSchema();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        try {
            handler.traceConnectionOperation( "setSchema(String)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.SCHEMA );
            c.setSchema( schema );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        try {
            handler.traceConnectionOperation( "getTypeMap()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.getTypeMap();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        try {
            handler.traceConnectionOperation( "setTypeMap(Map<String, Class<?>>)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            c.setTypeMap( map );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        try {
            handler.traceConnectionOperation( "getTransactionIsolation()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.getTransactionIsolation();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        try {
            handler.traceConnectionOperation( "setTransactionIsolation(int)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.TRANSACTION_ISOLATION );
            c.setTransactionIsolation( level );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        try {
            handler.traceConnectionOperation( "getWarnings()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.getWarnings();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        try {
            handler.traceConnectionOperation( "isClosed()" );
            return wrappedConnection.isClosed();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        try {
            handler.traceConnectionOperation( "isReadOnly()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.isReadOnly();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        try {
            handler.traceConnectionOperation( "setReadOnly(boolean)" );
            handler.verifyReadOnly( readOnly );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.READ_ONLY );
            c.setReadOnly( readOnly );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        try {
            handler.traceConnectionOperation( "isValid(int)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.isValid( timeout );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        try {
            handler.traceConnectionOperation( "nativeSQL(String)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.nativeSQL( sql );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareCall(String)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return trackCallableStatement( c.prepareCall( sql ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareCall(String, int, int)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return trackCallableStatement( c.prepareCall( sql, resultSetType, resultSetConcurrency ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareCall(String, int, int, int)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return trackCallableStatement( c.prepareCall( sql, resultSetType, resultSetConcurrency, resultSetHoldability ), resultSetHoldability == HOLD_CURSORS_OVER_COMMIT );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareStatement(String)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return trackPreparedStatement( c.prepareStatement( sql ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareStatement(String, int, int)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return trackPreparedStatement( c.prepareStatement( sql, resultSetType, resultSetConcurrency ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareStatement(String, int, int, int)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return trackPreparedStatement( c.prepareStatement( sql, resultSetType, resultSetConcurrency, resultSetHoldability ), resultSetHoldability == HOLD_CURSORS_OVER_COMMIT );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareStatement(String, int)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return trackPreparedStatement( c.prepareStatement( sql, autoGeneratedKeys ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareStatement(String, int[])" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return trackPreparedStatement( c.prepareStatement( sql, columnIndexes ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareStatement(String, String[])" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return trackPreparedStatement( c.prepareStatement( sql, columnNames ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        try {
            handler.traceConnectionOperation( "releaseSavepoint(Savepoint)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            c.releaseSavepoint( savepoint );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            handler.traceConnectionOperation( "setClientInfo(String, String)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            c.setClientInfo( name, value );
        } catch ( SQLClientInfoException sce ) {
            handler.setFlushOnly( sce );
            throw sce;
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw new SQLClientInfoException( Collections.singletonMap( name, REASON_UNKNOWN ) );
        }
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        try {
            handler.traceConnectionOperation( "setSavepoint()" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.setSavepoint();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        try {
            handler.traceConnectionOperation( "setSavepoint(String)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            return c.setSavepoint( name );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        try {
            handler.traceConnectionOperation( "setNetworkTimeout(Executor, int)" );
            Connection c = getWrappedConnection();
            verifyEnlistment();
            handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.NETWORK_TIMEOUT );
            c.setNetworkTimeout( executor, milliseconds );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    // --- //

    @Override
    @SuppressWarnings( "unchecked" )
    public <T> T unwrap(Class<T> target) throws SQLException {
        try {
            handler.traceConnectionOperation( "unwrap(Class<T>)" );
            return getWrappedConnection().unwrap( target );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> target) throws SQLException {
        try {
            handler.traceConnectionOperation( "isWrapperFor(Class<?>)" );
            return getWrappedConnection().isWrapperFor( target );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String toString() {
        return "wrapped[" + wrappedConnection + ( handler.isEnlisted() ? "]<<enrolled" : "]" );
    }

    public void addLeakedStatements(int leaks) {
        leakedStatements += leaks;
    }

    public int getLeakedStatements() {
        return leakedStatements;
    }

    public void addLeakedResultSets(int leaks) {
        leakedResultSets += leaks;
    }

    public int getLeakedResultSets() {
        return leakedResultSets;
    }

    // --- //

    void pruneClosedStatements() {
        if ( trackedStatements != null ) {
            trackedStatements.pruneClosed();
            if ( trackedStatements.isElementListEmpty() ) {
                holdState = false; // unset hold status
            }
        }
    }

    @Override
    public boolean isHeld() {
        return holdState;
    }

    @Override
    protected boolean internalClosed() {
        return wrappedConnection == CLOSED_CONNECTION;
    }
}
