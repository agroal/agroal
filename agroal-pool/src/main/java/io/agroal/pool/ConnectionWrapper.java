// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.configuration.InterruptProtection;
import io.agroal.api.transaction.TransactionAware;

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
 */
public class ConnectionWrapper implements Connection, TransactionAware {

    private static final InvocationHandler CLOSED_HANDLER = (proxy, method, args) -> {
        String methodName = method.getName();
        if ( "abort".equals( methodName ) ) {
            return Void.TYPE;
        } else if ( "isClosed".equals( methodName ) ) {
            return Boolean.TRUE;
        } else if ( "isValid".equals( methodName ) ) {
            return Boolean.FALSE;
        } else if ( "toString".equals( methodName ) ) {
            return ConnectionWrapper.class.getCanonicalName() + ".CLOSED_CONNECTION";
        }
        throw new SQLException( "Connection is closed" );
    };

    private static final Connection CLOSED_CONNECTION = (Connection) newProxyInstance( Connection.class.getClassLoader(), new Class[]{Connection.class}, CLOSED_HANDLER );

    // --- //

    private final ConnectionHandler handler;
    private final InterruptProtection interruptProtection;
    private Connection wrappedConnection;
    private boolean inTransaction;
    private boolean autocommitStashValue;

    public ConnectionWrapper(ConnectionHandler connectionHandler, InterruptProtection protection) {
        handler = connectionHandler;
        interruptProtection = protection;
        wrappedConnection = connectionHandler.getConnection();
        inTransaction = false;
    }

    // --- //

    @Override
    public void transactionStart() throws SQLException {
        autocommitStashValue = wrappedConnection.getAutoCommit();
        wrappedConnection.setAutoCommit( false );
        inTransaction = true;
    }

    @Override
    public void transactionCommit() throws SQLException {
        protect( () -> wrappedConnection.commit() );
    }

    // --- //

    @Override
    public void transactionRollback() throws SQLException {
        protect( () -> wrappedConnection.rollback() );
    }

    @Override
    public void transactionEnd() throws SQLException {
        inTransaction = false;
        wrappedConnection.setAutoCommit( autocommitStashValue );
    }

    // --- //

    private <T> T protect(InterruptProtection.SQLCallable<T> callable) throws SQLException {
        return interruptProtection.protect( callable );
    }

    private void protect(InterruptProtection.SQLRunnable runnable) throws SQLException {
        interruptProtection.protect( runnable );
    }

    public ConnectionHandler getHandler() {
        return handler;
    }

    @Override
    public void close() throws SQLException {
        wrappedConnection = CLOSED_CONNECTION;
        handler.returnConnection();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        wrappedConnection.abort( executor );
    }

    @Override
    public void commit() throws SQLException {
        if ( inTransaction ) {
            throw new SQLException( "Attempting to commit while taking part in a transaction" );
        }
        protect( () -> wrappedConnection.commit() );
    }

    @Override
    public void clearWarnings() throws SQLException {
        wrappedConnection.clearWarnings();
    }

    @Override
    public Clob createClob() throws SQLException {
        return wrappedConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return wrappedConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return wrappedConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return wrappedConnection.createSQLXML();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return wrappedConnection.createArrayOf( typeName, elements );
    }

    @Override
    public Statement createStatement() throws SQLException {
        return wrappedConnection.createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return wrappedConnection.createStatement( resultSetType, resultSetConcurrency );
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return wrappedConnection.createStatement( resultSetType, resultSetConcurrency, resultSetHoldability );
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return wrappedConnection.createStruct( typeName, attributes );
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return wrappedConnection.getAutoCommit();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if ( autoCommit && inTransaction ) {
            throw new SQLException( "Trying to set autocommit in connection taking part of transaction" );
        }
        wrappedConnection.setAutoCommit( autoCommit );
    }

    @Override
    public String getCatalog() throws SQLException {
        return wrappedConnection.getCatalog();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        wrappedConnection.setCatalog( catalog );
    }

    @Override
    public int getHoldability() throws SQLException {
        return wrappedConnection.getHoldability();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        wrappedConnection.setHoldability( holdability );
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return wrappedConnection.getClientInfo();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        wrappedConnection.setClientInfo( properties );
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return wrappedConnection.getClientInfo( name );
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return wrappedConnection.getMetaData();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return wrappedConnection.getNetworkTimeout();
    }

    @Override
    public String getSchema() throws SQLException {
        return wrappedConnection.getSchema();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        wrappedConnection.setSchema( schema );
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return wrappedConnection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        wrappedConnection.setTypeMap( map );
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return wrappedConnection.getTransactionIsolation();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        wrappedConnection.setTransactionIsolation( level );
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return wrappedConnection.getWarnings();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return wrappedConnection.isClosed();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return wrappedConnection.isReadOnly();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        wrappedConnection.setReadOnly( readOnly );
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return wrappedConnection.isValid( timeout );
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return wrappedConnection.nativeSQL( sql );
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return wrappedConnection.prepareCall( sql );
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return wrappedConnection.prepareCall( sql, resultSetType, resultSetConcurrency );
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return wrappedConnection.prepareCall( sql, resultSetType, resultSetConcurrency, resultSetHoldability );
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return wrappedConnection.prepareStatement( sql );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return wrappedConnection.prepareStatement( sql, resultSetType, resultSetConcurrency );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return wrappedConnection.prepareStatement( sql, resultSetType, resultSetConcurrency, resultSetHoldability );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return wrappedConnection.prepareStatement( sql, autoGeneratedKeys );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return wrappedConnection.prepareStatement( sql, columnIndexes );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return wrappedConnection.prepareStatement( sql, columnNames );
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        wrappedConnection.releaseSavepoint( savepoint );
    }

    @Override
    public void rollback() throws SQLException {
        if ( inTransaction ) {
            throw new SQLException( "Attempting to rollback while enlisted in a transaction" );
        }
        protect( () -> wrappedConnection.rollback() );
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        if ( inTransaction ) {
            throw new SQLException( "Attempting to commit while enlisted in a transaction" );
        }
        protect( () -> wrappedConnection.rollback( savepoint ) );
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        wrappedConnection.setClientInfo( name, value );
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return wrappedConnection.setSavepoint();
    }

    // --- //

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return wrappedConnection.setSavepoint( name );
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
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
}
