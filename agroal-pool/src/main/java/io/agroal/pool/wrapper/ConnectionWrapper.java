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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static java.lang.reflect.Proxy.newProxyInstance;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public final class ConnectionWrapper extends AutoCloseableElement implements Connection {

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

    private static final JdbcResourcesLeakReport JDBC_RESOURCES_NOT_LEAKED = new JdbcResourcesLeakReport( 0, 0 );

    // --- //

    // Connection.close() does not return the connection to the pool.
    private final boolean detached;

    // Collection of Statements to close them on close(). If null Statements are not tracked.
    private final AutoCloseableElement trackedStatements;
    private int leakedResultSets;

    private final ConnectionHandler handler;

    private Connection wrappedConnection;

    public ConnectionWrapper(ConnectionHandler connectionHandler, boolean trackResources) {
        this(connectionHandler, trackResources, false );
    }

    public ConnectionWrapper(ConnectionHandler connectionHandler, boolean trackResources, boolean detached) {
        super( null );
        handler = connectionHandler;
        wrappedConnection = connectionHandler.rawConnection();
        trackedStatements = trackResources ? AutoCloseableElement.newHead() : null;
        this.detached = detached;
    }

    public ConnectionWrapper(ConnectionHandler connectionHandler, boolean trackResources, AutoCloseableElement head ) {
        super( head );
        handler = connectionHandler;
        wrappedConnection = connectionHandler.rawConnection();
        trackedStatements = trackResources ? AutoCloseableElement.newHead() : null;
        detached = false;
    }

    public ConnectionHandler getHandler() {
        return handler;
    }

    public boolean isDetached() {
        return detached;
    }

    // --- //

    private Statement trackStatement(Statement statement) {
        if ( trackedStatements != null && statement != null ) {
            return new StatementWrapper( this, statement, true, trackedStatements );
        }
        return statement;
    }

    private CallableStatement trackCallableStatement(CallableStatement statement) {
        if ( trackedStatements != null && statement != null ) {
            return new CallableStatementWrapper( this, statement, true, trackedStatements );
        }
        return statement;
    }

    private PreparedStatement trackPreparedStatement(PreparedStatement statement) {
        if ( trackedStatements != null && statement != null ) {
            return new PreparedStatementWrapper( this, statement, true, trackedStatements );
        }
        return statement;
    }

    private JdbcResourcesLeakReport closeTrackedStatements() throws SQLException {
        if ( trackedStatements != null ) {
            return new JdbcResourcesLeakReport( trackedStatements.closeAllAutocloseableElements(), leakedResultSets );
        }
        return JDBC_RESOURCES_NOT_LEAKED;
    }

    // --- //

    @Override
    public void close() throws SQLException {
        handler.traceConnectionOperation( "close()" );
        if ( wrappedConnection != CLOSED_CONNECTION ) {
            wrappedConnection = CLOSED_CONNECTION;
            handler.onConnectionWrapperClose( this, closeTrackedStatements() );
        }
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        handler.traceConnectionOperation( "abort()" );
        try {
            wrappedConnection = CLOSED_CONNECTION;
            wrappedConnection.abort( executor );
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
            handler.verifyEnlistment();
            if ( wrappedConnection.getAutoCommit() != autoCommit ) {
                handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.AUTOCOMMIT );
                wrappedConnection.setAutoCommit( autoCommit );
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
            handler.verifyEnlistment();
            return wrappedConnection.getAutoCommit();
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
            wrappedConnection.commit();
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
            handler.verifyEnlistment();
            wrappedConnection.rollback();
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
            handler.verifyEnlistment();
            wrappedConnection.rollback( savepoint );
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
            handler.verifyEnlistment();
            wrappedConnection.clearWarnings();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Clob createClob() throws SQLException {
        try {
            handler.traceConnectionOperation( "createClob()" );
            handler.verifyEnlistment();
            return wrappedConnection.createClob();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Blob createBlob() throws SQLException {
        try {
            handler.traceConnectionOperation( "createBlob()" );
            handler.verifyEnlistment();
            return wrappedConnection.createBlob();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public NClob createNClob() throws SQLException {
        try {
            handler.traceConnectionOperation( "createNClob()" );
            handler.verifyEnlistment();
            return wrappedConnection.createNClob();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        try {
            handler.traceConnectionOperation( "createSQLXML()" );
            handler.verifyEnlistment();
            return wrappedConnection.createSQLXML();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        try {
            handler.traceConnectionOperation( "createArrayOf(String, Object[])" );
            handler.verifyEnlistment();
            return wrappedConnection.createArrayOf( typeName, elements );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        try {
            handler.traceConnectionOperation( "createStatement()" );
            handler.verifyEnlistment();
            return trackStatement( wrappedConnection.createStatement() );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            handler.traceConnectionOperation( "createStatement(int, int)" );
            handler.verifyEnlistment();
            return trackStatement( wrappedConnection.createStatement( resultSetType, resultSetConcurrency ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        try {
            handler.traceConnectionOperation( "createStatement(int, int, int)" );
            handler.verifyEnlistment();
            return trackStatement( wrappedConnection.createStatement( resultSetType, resultSetConcurrency, resultSetHoldability ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        try {
            handler.traceConnectionOperation( "createStruct(String, Object[])" );
            handler.verifyEnlistment();
            return wrappedConnection.createStruct( typeName, attributes );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String getCatalog() throws SQLException {
        try {
            handler.traceConnectionOperation( "getCatalog()" );
            handler.verifyEnlistment();
            return wrappedConnection.getCatalog();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        try {
            handler.traceConnectionOperation( "setCatalog(String)" );
            handler.verifyEnlistment();
            handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.CATALOG );
            wrappedConnection.setCatalog( catalog );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        try {
            handler.traceConnectionOperation( "getHoldability()" );
            handler.verifyEnlistment();
            return wrappedConnection.getHoldability();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        try {
            handler.traceConnectionOperation( "setHoldability(int)" );
            handler.verifyEnlistment();
            wrappedConnection.setHoldability( holdability );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        try {
            handler.traceConnectionOperation( "getClientInfo()" );
            handler.verifyEnlistment();
            return wrappedConnection.getClientInfo();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            handler.traceConnectionOperation( "setClientInfo(Properties)" );
            wrappedConnection.setClientInfo( properties );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        try {
            handler.traceConnectionOperation( "getClientInfo(String)" );
            handler.verifyEnlistment();
            return wrappedConnection.getClientInfo( name );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        try {
            handler.traceConnectionOperation( "getMetaData()" );
            handler.verifyEnlistment();
            return wrappedConnection.getMetaData();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        try {
            handler.traceConnectionOperation( "getNetworkTimeout()" );
            handler.verifyEnlistment();
            return wrappedConnection.getNetworkTimeout();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String getSchema() throws SQLException {
        try {
            handler.traceConnectionOperation( "getSchema()" );
            handler.verifyEnlistment();
            return wrappedConnection.getSchema();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        try {
            handler.traceConnectionOperation( "setSchema(String)" );
            handler.verifyEnlistment();
            handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.SCHEMA );
            wrappedConnection.setSchema( schema );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        try {
            handler.traceConnectionOperation( "getTypeMap()" );
            handler.verifyEnlistment();
            return wrappedConnection.getTypeMap();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        try {
            handler.traceConnectionOperation( "setTypeMap(Map<String, Class<?>>)" );
            handler.verifyEnlistment();
            wrappedConnection.setTypeMap( map );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        try {
            handler.traceConnectionOperation( "getTransactionIsolation()" );
            handler.verifyEnlistment();
            return wrappedConnection.getTransactionIsolation();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        try {
            handler.traceConnectionOperation( "setTransactionIsolation(int)" );
            handler.verifyEnlistment();
            handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.TRANSACTION_ISOLATION );
            wrappedConnection.setTransactionIsolation( level );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        try {
            handler.traceConnectionOperation( "getWarnings()" );
            handler.verifyEnlistment();
            return wrappedConnection.getWarnings();
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
            handler.verifyEnlistment();
            return wrappedConnection.isReadOnly();
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
            handler.verifyEnlistment();
            handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.READ_ONLY );
            wrappedConnection.setReadOnly( readOnly );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        try {
            handler.traceConnectionOperation( "isValid(int)" );
            handler.verifyEnlistment();
            return wrappedConnection.isValid( timeout );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        try {
            handler.traceConnectionOperation( "nativeSQL(String)" );
            handler.verifyEnlistment();
            return wrappedConnection.nativeSQL( sql );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareCall(String)" );
            handler.verifyEnlistment();
            return trackCallableStatement( wrappedConnection.prepareCall( sql ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareCall(String, int, int)" );
            handler.verifyEnlistment();
            return trackCallableStatement( wrappedConnection.prepareCall( sql, resultSetType, resultSetConcurrency ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareCall(String, int, int, int)" );
            handler.verifyEnlistment();
            return trackCallableStatement( wrappedConnection.prepareCall( sql, resultSetType, resultSetConcurrency, resultSetHoldability ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareStatement(String)" );
            handler.verifyEnlistment();
            return trackPreparedStatement( wrappedConnection.prepareStatement( sql ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareStatement(String, int, int)" );
            handler.verifyEnlistment();
            return trackPreparedStatement( wrappedConnection.prepareStatement( sql, resultSetType, resultSetConcurrency ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareStatement(String, int, int, int)" );
            handler.verifyEnlistment();
            return trackPreparedStatement( wrappedConnection.prepareStatement( sql, resultSetType, resultSetConcurrency, resultSetHoldability ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareStatement(String, int)" );
            handler.verifyEnlistment();
            return trackPreparedStatement( wrappedConnection.prepareStatement( sql, autoGeneratedKeys ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareStatement(String, int[])" );
            handler.verifyEnlistment();
            return trackPreparedStatement( wrappedConnection.prepareStatement( sql, columnIndexes ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        try {
            handler.traceConnectionOperation( "prepareStatement(String, String[])" );
            handler.verifyEnlistment();
            return trackPreparedStatement( wrappedConnection.prepareStatement( sql, columnNames ) );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        try {
            handler.traceConnectionOperation( "releaseSavepoint(Savepoint)" );
            handler.verifyEnlistment();
            wrappedConnection.releaseSavepoint( savepoint );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            handler.traceConnectionOperation( "setClientInfo(String, String)" );
            wrappedConnection.setClientInfo( name, value );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        try {
            handler.traceConnectionOperation( "setSavepoint()" );
            handler.verifyEnlistment();
            return wrappedConnection.setSavepoint();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        try {
            handler.traceConnectionOperation( "setSavepoint(String)" );
            handler.verifyEnlistment();
            return wrappedConnection.setSavepoint( name );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        try {
            handler.traceConnectionOperation( "setNetworkTimeout(Executor, int)" );
            handler.verifyEnlistment();
            handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.NETWORK_TIMEOUT );
            wrappedConnection.setNetworkTimeout( executor, milliseconds );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    // --- //

    @Override
    public <T> T unwrap(Class<T> target) throws SQLException {
        try {
            handler.traceConnectionOperation( "unwrap(Class<T>)" );
            return wrappedConnection.unwrap( target );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> target) throws SQLException {
        try {
            handler.traceConnectionOperation( "isWrapperFor(Class<?>)" );
            return wrappedConnection.isWrapperFor( target );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String toString() {
        return "wrapped[" + wrappedConnection + ( handler.isEnlisted() ? "]<<enrolled" : "]" );
    }

    public void addLeakedResultSets(int leaks) {
       leakedResultSets += leaks;
    }

    // --- //

    public static class JdbcResourcesLeakReport {

        private final int statementCount;

        private final int resultSetCount;

        @SuppressWarnings( "WeakerAccess" )
        JdbcResourcesLeakReport(int statementCount, int resultSetCount) {
            this.statementCount = statementCount;
            this.resultSetCount = resultSetCount;
        }

        public int statementCount() {
            return statementCount;
        }

        public int resultSetCount() {
            return resultSetCount;
        }

        public boolean hasLeak() {
            return statementCount != 0 || resultSetCount != 0;
        }
    }

    void pruneClosedStatements() {
        if (trackedStatements != null) {
            trackedStatements.pruneClosed();
        }
    }

    @Override
    protected boolean internalClosed() {
        return wrappedConnection == CLOSED_CONNECTION;
    }
}
