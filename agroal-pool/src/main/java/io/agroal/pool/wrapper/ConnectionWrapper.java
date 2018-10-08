// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper;

import io.agroal.pool.ConnectionHandler;
import io.agroal.pool.util.StampedCopyOnWriteArrayList;

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
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static java.lang.reflect.Proxy.newProxyInstance;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class ConnectionWrapper implements Connection {

    private static final InvocationHandler CLOSED_HANDLER = (proxy, method, args) -> {
        switch ( method.getName() ) {
            case "abort":
                return Void.TYPE;
            case "close":
                return Void.TYPE;
            case "isClosed":
                return Boolean.TRUE;
            case "isValid":
                return Boolean.FALSE;
            case "toString":
                return ConnectionWrapper.class.getSimpleName() + ".CLOSED_CONNECTION";
            default:
                throw new SQLException( "Connection is closed" );
        }
    };

    private static final Connection CLOSED_CONNECTION = (Connection) newProxyInstance( Connection.class.getClassLoader(), new Class[]{Connection.class}, CLOSED_HANDLER );

    // --- //

    private final Collection<Statement> trackedStatements = new StampedCopyOnWriteArrayList<>( Statement.class );

    private final ConnectionHandler handler;

    private Connection wrappedConnection;

    // TODO: make trackStatements configurable
    // Flag to indicate that this ConnectionWrapper should track statements to close them on close()
    private boolean trackStatements = true;

    public ConnectionWrapper(ConnectionHandler connectionHandler) {
        handler = connectionHandler;
        wrappedConnection = connectionHandler.getConnection();
    }

    public ConnectionHandler getHandler() {
        return handler;
    }

    // --- //

    private Statement trackStatement(Statement statement) {
        if ( trackStatements && statement != null ) {
            Statement wrappedStatement = new StatementWrapper( this, statement );
            trackedStatements.add( wrappedStatement );
            return wrappedStatement;
        }
        return statement;
    }

    private CallableStatement trackCallableStatement(CallableStatement statement) {
        if ( trackStatements && statement != null ) {
            CallableStatement wrappedStatement = new CallableStatementWrapper( this, statement );
            trackedStatements.add( wrappedStatement );
            return wrappedStatement;
        }
        return statement;
    }

    private PreparedStatement trackPreparedStatement(PreparedStatement statement) {
        if ( trackStatements && statement != null ) {
            PreparedStatement wrappedStatement = new PreparedStatementWrapper( this, statement );
            trackedStatements.add( wrappedStatement );
            return wrappedStatement;
        }
        return statement;
    }

    private void closeTrackedStatements() throws SQLException {
        if ( !trackedStatements.isEmpty() ) {
            for ( Statement statement : trackedStatements ) {
                statement.close();
            }
            // Statements remove themselves, but clear the collection anyway
            trackedStatements.clear();
        }
    }

    public void releaseTrackedStatement(Statement statement) {
        trackedStatements.remove( statement );
    }

    // --- //

    @Override
    public void close() throws SQLException {
        if ( wrappedConnection != CLOSED_CONNECTION ) {
            wrappedConnection = CLOSED_CONNECTION;
            closeTrackedStatements();
            handler.onConnectionWrapperClose( this );
        }
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        wrappedConnection = CLOSED_CONNECTION;
        wrappedConnection.abort( executor );
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.getAutoCommit();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if ( autoCommit && handler.isEnlisted() ) {
            throw new SQLException( "Trying to set autocommit in connection taking part of transaction" );
        }
        handler.deferredEnlistmentCheck();
        if ( wrappedConnection.getAutoCommit() != autoCommit ) {
            handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.AUTOCOMMIT );
            wrappedConnection.setAutoCommit( autoCommit );
        }
    }

    @Override
    public void commit() throws SQLException {
        if ( handler.isEnlisted() ) {
            throw new SQLException( "Attempting to commit while taking part in a transaction" );
        }
        wrappedConnection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        if ( handler.isEnlisted() ) {
            throw new SQLException( "Attempting to rollback while enlisted in a transaction" );
        }
        handler.deferredEnlistmentCheck();
        wrappedConnection.rollback();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        if ( handler.isEnlisted() ) {
            throw new SQLException( "Attempting to commit while enlisted in a transaction" );
        }
        handler.deferredEnlistmentCheck();
        wrappedConnection.rollback( savepoint );
    }

    // --- //

    @Override
    public void clearWarnings() throws SQLException {
        handler.deferredEnlistmentCheck();
        wrappedConnection.clearWarnings();
    }

    @Override
    public Clob createClob() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.createSQLXML();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.createArrayOf( typeName, elements );
    }

    @Override
    public Statement createStatement() throws SQLException {
        handler.deferredEnlistmentCheck();
        return trackStatement( wrappedConnection.createStatement() );
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        handler.deferredEnlistmentCheck();
        return trackStatement( wrappedConnection.createStatement( resultSetType, resultSetConcurrency ) );
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        handler.deferredEnlistmentCheck();
        return trackStatement( wrappedConnection.createStatement( resultSetType, resultSetConcurrency, resultSetHoldability ) );
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.createStruct( typeName, attributes );
    }

    @Override
    public String getCatalog() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.getCatalog();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        handler.deferredEnlistmentCheck();
        handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.CATALOG );
        wrappedConnection.setCatalog( catalog );
    }

    @Override
    public int getHoldability() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.getHoldability();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        handler.deferredEnlistmentCheck();
        wrappedConnection.setHoldability( holdability );
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.getClientInfo();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        wrappedConnection.setClientInfo( properties );
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.getClientInfo( name );
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.getMetaData();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.getNetworkTimeout();
    }

    @Override
    public String getSchema() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.getSchema();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        handler.deferredEnlistmentCheck();
        handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.SCHEMA );
        wrappedConnection.setSchema( schema );
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        handler.deferredEnlistmentCheck();
        wrappedConnection.setTypeMap( map );
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.getTransactionIsolation();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        handler.deferredEnlistmentCheck();
        handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.TRANSACTION_ISOLATION );
        wrappedConnection.setTransactionIsolation( level );
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.getWarnings();
    }

    @Override
    public boolean isClosed() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.isClosed();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.isReadOnly();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        handler.deferredEnlistmentCheck();
        wrappedConnection.setReadOnly( readOnly );
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.isValid( timeout );
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.nativeSQL( sql );
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        handler.deferredEnlistmentCheck();
        return trackCallableStatement( wrappedConnection.prepareCall( sql ) );
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        handler.deferredEnlistmentCheck();
        return trackCallableStatement( wrappedConnection.prepareCall( sql, resultSetType, resultSetConcurrency ) );
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        handler.deferredEnlistmentCheck();
        return trackCallableStatement( wrappedConnection.prepareCall( sql, resultSetType, resultSetConcurrency, resultSetHoldability ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        handler.deferredEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        handler.deferredEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql, resultSetType, resultSetConcurrency ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        handler.deferredEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql, resultSetType, resultSetConcurrency, resultSetHoldability ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        handler.deferredEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql, autoGeneratedKeys ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        handler.deferredEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql, columnIndexes ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        handler.deferredEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql, columnNames ) );
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        handler.deferredEnlistmentCheck();
        wrappedConnection.releaseSavepoint( savepoint );
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        wrappedConnection.setClientInfo( name, value );
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        handler.deferredEnlistmentCheck();
        return wrappedConnection.setSavepoint( name );
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        handler.deferredEnlistmentCheck();
        handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.NETWORK_TIMEOUT );
        wrappedConnection.setNetworkTimeout( executor, milliseconds );
    }

    // --- //

    @Override
    public <T> T unwrap(Class<T> target) throws SQLException {
        return wrappedConnection.unwrap( target );
    }

    @Override
    public boolean isWrapperFor(Class<?> target) throws SQLException {
        return wrappedConnection.isWrapperFor( target );
    }

    @Override
    public String toString() {
        return "wrapped[" + wrappedConnection + ( handler.isEnlisted() ? "]<<enrolled" : "]" );
    }
}
