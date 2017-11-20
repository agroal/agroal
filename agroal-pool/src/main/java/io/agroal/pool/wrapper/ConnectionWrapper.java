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

    private static final TransactionAware.SQLCallable<Boolean> NO_ACTIVE_TRANSACTION = () -> false;

    // --- //

    private final Collection<Statement> trackedStatements = new StampedCopyOnWriteArrayList<>( Statement.class );

    private final ConnectionHandler handler;

    private Connection wrappedConnection;

    // Flag to indicate that this ConnectionWrapper is currently enlisted with a transaction
    private boolean inTransaction = false;

    // This field is used to store temporarily the value of the autoCommit flag, while the connection is taking part on the transaction
    private boolean autocommitStash;

    // TODO: make trackStatements configurable
    // Flag to indicate that this ConnectionWrapper should track statements to close them on close()
    private boolean trackStatements = true;

    // Callback set by the transaction integration layer to prevent lazy enlistment
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
        autocommitStash = wrappedConnection.getAutoCommit();
        wrappedConnection.setAutoCommit( false );
        inTransaction = true;
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
        inTransaction = false;
        if ( wrappedConnection != CLOSED_CONNECTION ) {
            wrappedConnection.setAutoCommit( autocommitStash );
        }
    }

    @Override
    public Object getConnection() {
        ConnectionWrapper connection = new ConnectionWrapper( handler );
        connection.inTransaction = true;
        return connection;
    }

    @Override
    public void transactionCheckCallback(SQLCallable<Boolean> transactionCheck) {
        this.transactionActiveCheck = transactionCheck;
    }

    private void lazyEnlistmentCheck() throws SQLException {
        if ( !inTransaction && transactionActiveCheck.call() ) {
            throw new SQLException( "Lazy enlistment not supported" );
        }
    }

    // --- //

    private Statement trackStatement(Statement statement) {
        if ( trackStatements ) {
            Statement wrappedStatement = new StatementWrapper( this, statement );
            trackedStatements.add( wrappedStatement );
            return wrappedStatement;
        }
        return statement;
    }

    private CallableStatement trackCallableStatement(CallableStatement statement) {
        if ( trackStatements ) {
            CallableStatement wrappedStatement = new CallableStatementWrapper( this, statement );
            trackedStatements.add( wrappedStatement );
            return wrappedStatement;
        }
        return statement;
    }

    private PreparedStatement trackPreparedStatement(PreparedStatement statement) {
        if ( trackStatements ) {
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
        if ( !inTransaction ) {
            closeTrackedStatements();
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
        if ( autoCommit && inTransaction ) {
            throw new SQLException( "Trying to set autocommit in connection taking part of transaction" );
        }
        lazyEnlistmentCheck();
        handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.AUTOCOMMIT );
        wrappedConnection.setAutoCommit( autoCommit );
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        if ( inTransaction ) {
            throw new SQLException( "Attempting to commit while taking part in a transaction" );
        }
        wrappedConnection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        if ( inTransaction ) {
            throw new SQLException( "Attempting to rollback while enlisted in a transaction" );
        }
        lazyEnlistmentCheck();
        wrappedConnection.rollback();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        if ( inTransaction ) {
            throw new SQLException( "Attempting to commit while enlisted in a transaction" );
        }
        lazyEnlistmentCheck();
        wrappedConnection.rollback( savepoint );
    }

    // --- //

    @Override
    public void clearWarnings() throws SQLException {
        lazyEnlistmentCheck();
        wrappedConnection.clearWarnings();
    }

    @Override
    public Clob createClob() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.createSQLXML();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.createArrayOf( typeName, elements );
    }

    @Override
    public Statement createStatement() throws SQLException {
        lazyEnlistmentCheck();
        return trackStatement( wrappedConnection.createStatement() );
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        lazyEnlistmentCheck();
        return trackStatement( wrappedConnection.createStatement( resultSetType, resultSetConcurrency ) );
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        lazyEnlistmentCheck();
        return trackStatement( wrappedConnection.createStatement( resultSetType, resultSetConcurrency, resultSetHoldability ) );
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.createStruct( typeName, attributes );
    }

    @Override
    public String getCatalog() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.getCatalog();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        lazyEnlistmentCheck();
        handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.CATALOG );
        wrappedConnection.setCatalog( catalog );
    }

    @Override
    public int getHoldability() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.getHoldability();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        lazyEnlistmentCheck();
        wrappedConnection.setHoldability( holdability );
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.getClientInfo();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        wrappedConnection.setClientInfo( properties );
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.getClientInfo( name );
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.getMetaData();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.getNetworkTimeout();
    }

    @Override
    public String getSchema() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.getSchema();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        lazyEnlistmentCheck();
        handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.SCHEMA );
        wrappedConnection.setSchema( schema );
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        lazyEnlistmentCheck();
        wrappedConnection.setTypeMap( map );
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.getTransactionIsolation();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        lazyEnlistmentCheck();
        handler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.TRANSACTION_ISOLATION );
        wrappedConnection.setTransactionIsolation( level );
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.getWarnings();
    }

    @Override
    public boolean isClosed() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.isClosed();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.isReadOnly();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        lazyEnlistmentCheck();
        wrappedConnection.setReadOnly( readOnly );
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.isValid( timeout );
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.nativeSQL( sql );
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        lazyEnlistmentCheck();
        return trackCallableStatement( wrappedConnection.prepareCall( sql ) );
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        lazyEnlistmentCheck();
        return trackCallableStatement( wrappedConnection.prepareCall( sql, resultSetType, resultSetConcurrency ) );
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        lazyEnlistmentCheck();
        return trackCallableStatement( wrappedConnection.prepareCall( sql, resultSetType, resultSetConcurrency, resultSetHoldability ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        lazyEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        lazyEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql, resultSetType, resultSetConcurrency ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        lazyEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql, resultSetType, resultSetConcurrency, resultSetHoldability ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        lazyEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql, autoGeneratedKeys ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        lazyEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql, columnIndexes ) );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        lazyEnlistmentCheck();
        return trackPreparedStatement( wrappedConnection.prepareStatement( sql, columnNames ) );
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        lazyEnlistmentCheck();
        wrappedConnection.releaseSavepoint( savepoint );
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        wrappedConnection.setClientInfo( name, value );
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        lazyEnlistmentCheck();
        return wrappedConnection.setSavepoint( name );
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        lazyEnlistmentCheck();
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
        return "wrapped[" + wrappedConnection + ( inTransaction ? "]<<enrolled" : "]" );
    }
}
