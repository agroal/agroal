// Copyright (C) 2026 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper.closed;

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

import static java.sql.ClientInfoStatus.REASON_UNKNOWN;

/**
 * Sentinel implementation of {@link Connection} that throws {@link SQLException} on all operations.
 *
 * @author <a href="gegastaldi@gmail.com">George Gastaldi</a>
 */
public final class ClosedConnection implements Connection {

    public static final ClosedConnection INSTANCE = new ClosedConnection();

    // --- //

    private ClosedConnection() {
    }

    private static SQLException closed() {
        return new SQLException( "Connection is closed" );
    }

    @Override
    public Statement createStatement() throws SQLException {
        throw closed();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw closed();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw closed();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw closed();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        throw closed();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        throw closed();
    }

    @Override
    public void commit() throws SQLException {
        throw closed();
    }

    @Override
    public void rollback() throws SQLException {
        throw closed();
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public boolean isClosed() {
        return true;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw closed();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw closed();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw closed();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw closed();
    }

    @Override
    public String getCatalog() throws SQLException {
        throw closed();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw closed();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw closed();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw closed();
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw closed();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw closed();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw closed();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw closed();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw closed();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw closed();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw closed();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw closed();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw closed();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw closed();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw closed();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw closed();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw closed();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw closed();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw closed();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw closed();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw closed();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw closed();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw closed();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw closed();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw closed();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw closed();
    }

    @Override
    public boolean isValid(int timeout) {
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException( "Connection is closed", Collections.singletonMap( name, REASON_UNKNOWN ) );
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException( "Connection is closed", Collections.emptyMap() );
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw closed();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw closed();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw closed();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw closed();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw closed();
    }

    @Override
    public String getSchema() throws SQLException {
        throw closed();
    }

    @Override
    public void abort(Executor executor) {
        // no-op
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw closed();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw closed();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw closed();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw closed();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
