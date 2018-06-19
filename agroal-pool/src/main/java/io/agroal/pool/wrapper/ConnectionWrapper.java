// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper;

import io.agroal.api.transaction.TransactionAware;
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
public final class ConnectionWrapper implements Connection, TransactionAware {

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

    private static final TransactionAware.SQLCallable<Boolean> NO_ACTIVE_TRANSACTION = new SQLCallable<Boolean>() {
        @Override
        public Boolean call() throws SQLException {
            return false;
        }
    };

    // --- //

    private final Collection<Statement> trackedStatements = new StampedCopyOnWriteArrayList<>( Statement.class );

    private final ConnectionHandler handler;

    private Connection wrappedConnection;

    // This boolean prevents the connection to be returned to the pool multiple times
    private boolean returnedHandler = false;

    // TODO: make trackStatements configurable
    // Flag to indicate that this ConnectionWrapper should track statements to close them on close()
    private boolean trackStatements = true;

    // Callback set by the transaction integration layer to prevent deferred enlistment
    // If the connection is not associated with a transaction and an operation occurs within the bounds of a transaction, an SQLException is thrown
    // If there is no transaction integration this should just return false
    private TransactionAware.SQLCallable<Boolean> transactionActiveCheck = NO_ACTIVE_TRANSACTION;

    public ConnectionWrapper(ConnectionHandler connectionHandler) {
        handler = connectionHandler;
        wrappedConnection = connectionHandler.getConnection();
    }

    public ConnectionHandler getHandler() {
        return handler;
    }

    // --- //

    @Override
    public void transactionStart() throws SQLException {
        if ( !handler.isEnlisted() ) {
              handler.setEnlisted();
              setAutoCommit( false );
        }
    }

    @Override
    public void transactionCommit() throws SQLException {
        handler.getConnection().commit();
    }

    @Override
    public void transactionRollback() throws SQLException {
        handler.getConnection().rollback();
    }

    @Override
    public void transactionEnd() throws SQLException {
        handler.resetEnlisted();
    }

    @Override
    public Object getConnection() {
        return new ConnectionWrapper( handler );
    }

    @Override
    public void transactionCheckCallback(SQLCallable<Boolean> transactionCheck) {
        this.transactionActiveCheck = transactionCheck;
    }

    private void deferredEnlistmentCheck() throws SQLException {
        if ( !handler.isEnlisted() && transactionActiveCheck.call() ) {
            throw new SQLException( "Deferred enlistment not supported" );
        }
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
        wrappedConnection = CLOSED_CONNECTION;
        if ( !handler.isEnlisted() && !returnedHandler ) {
            returnedHandler = true;
            closeTrackedStatements();
            transactionActiveCheck = NO_ACTIVE_TRANSACTION;
            handler.returnConnection();
        }
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        wrappedConnection = CLOSED_CONNECTION;
        wrappedConnection.abort( executor );
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if ( autoCommit && handler.isEnlisted() ) {
            throw new SQLException( "Trying to set autocommit in connection taking part of transaction" );
        }
        deferredEnlistmentCheck();
        handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.AUTOCOMMIT );
        wrappedConnection.setAutoCommit( autoCommit );
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.getAutoCommit();
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
        deferredEnlistmentCheck();
        wrappedConnection.rollback();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        if ( handler.isEnlisted() ) {
            throw new SQLException( "Attempting to commit while enlisted in a transaction" );
        }
        deferredEnlistmentCheck();
        wrappedConnection.rollback( savepoint );
    }

    // --- //

    @Override
    public void clearWarnings() throws SQLException {
        deferredEnlistmentCheck();
        wrappedConnection.clearWarnings();
    }

    @Override
    public Clob createClob() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.createSQLXML();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.createArrayOf( typeName, elements );
    }

    @Override
    public Statement createStatement() throws SQLException {
        deferredEnlistmentCheck();
        return trackStatement( wrappedConnection.createStatement() );
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        deferredEnlistmentCheck();
        return trackStatement( wrappedConnection.createStatement( resultSetType, resultSetConcurrency ) );
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        deferredEnlistmentCheck();
        return trackStatement( wrappedConnection.createStatement( resultSetType, resultSetConcurrency, resultSetHoldability ) );
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.createStruct( typeName, attributes );
    }

    @Override
    public String getCatalog() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.getCatalog();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        deferredEnlistmentCheck();
        handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.CATALOG );
        wrappedConnection.setCatalog( catalog );
    }

    @Override
    public int getHoldability() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.getHoldability();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        deferredEnlistmentCheck();
        wrappedConnection.setHoldability( holdability );
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.getClientInfo();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        wrappedConnection.setClientInfo( properties );
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.getClientInfo( name );
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.getMetaData();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.getNetworkTimeout();
    }

    @Override
    public String getSchema() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.getSchema();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        deferredEnlistmentCheck();
        handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.SCHEMA );
        wrappedConnection.setSchema( schema );
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        deferredEnlistmentCheck();
        wrappedConnection.setTypeMap( map );
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.getTransactionIsolation();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        deferredEnlistmentCheck();
        handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.TRANSACTION_ISOLATION );
        wrappedConnection.setTransactionIsolation( level );
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.getWarnings();
    }

    @Override
    public boolean isClosed() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.isClosed();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.isReadOnly();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        deferredEnlistmentCheck();
        wrappedConnection.setReadOnly( readOnly );
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.isValid( timeout );
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.nativeSQL( sql );
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        deferredEnlistmentCheck();
        return trackCallableStatement( wrappedConnection.prepareCall( sql ) );
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        deferredEnlistmentCheck();
        return trackCallableStatement( wrappedConnection.prepareCall( sql, resultSetType, resultSetConcurrency ) );
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        deferredEnlistmentCheck();
        return trackCallableStatement( wrappedConnection.prepareCall( sql, resultSetType, resultSetConcurrency, resultSetHoldability ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        deferredEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        deferredEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql, resultSetType, resultSetConcurrency ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        deferredEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql, resultSetType, resultSetConcurrency, resultSetHoldability ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        deferredEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql, autoGeneratedKeys ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        deferredEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql, columnIndexes ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        deferredEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql, columnNames ) );
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        deferredEnlistmentCheck();
        wrappedConnection.releaseSavepoint( savepoint );
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        wrappedConnection.setClientInfo( name, value );
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        deferredEnlistmentCheck();
        return wrappedConnection.setSavepoint( name );
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        deferredEnlistmentCheck();
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
