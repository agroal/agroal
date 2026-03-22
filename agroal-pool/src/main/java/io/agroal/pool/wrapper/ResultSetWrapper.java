// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper;

import io.agroal.pool.ConnectionHandler;
import io.agroal.pool.util.AutoCloseableElement;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import static java.lang.reflect.Proxy.newProxyInstance;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public final class ResultSetWrapper extends AutoCloseableElement<ResultSetWrapper> implements ResultSet {

    static final String CLOSED_RESULT_SET_STRING = ResultSetWrapper.class.getSimpleName() + ".CLOSED_RESULT_SET";

    private static final InvocationHandler CLOSED_HANDLER = new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch ( method.getName() ) {
                case "close":
                    return Void.TYPE;
                case "isClosed":
                    return Boolean.TRUE;
                case "toString":
                    return CLOSED_RESULT_SET_STRING;
                default:
                    throw new SQLException( "ResultSet is closed" );
            }
        }
    };

    private static final ResultSet CLOSED_RESULT_SET = (ResultSet) newProxyInstance( ResultSet.class.getClassLoader(), new Class[]{ResultSet.class}, CLOSED_HANDLER );

    // --- //

    private final StatementWrapper statement;

    private final ConnectionHandler handler;

    private ResultSet wrappedResultSet;

    public ResultSetWrapper(StatementWrapper statementWrapper, ResultSet resultSet, AutoCloseableElement<ResultSetWrapper> head, boolean defaultHold) {
        super( head );
        statement = statementWrapper;
        handler = statementWrapper.getConnectionWrapper().getHandler();
        wrappedResultSet = resultSet;
    }

    /**
     * Acquires the read lock and verifies enlistment. Must be called before the try block so that
     * endOperation is only reached when the lock was successfully acquired.
     */
    private boolean beginOperation() throws SQLException {
        handler.readLock();
        boolean wasEnlisted = handler.isEnlisted();
        try {
            statement.getConnectionWrapper().verifyEnlistment();
        } catch ( SQLException se ) {
            handler.readUnlock();
            throw se;
        }
        return wasEnlisted;
    }

    /**
     * Releases the read lock after a result set operation completes.
     * If the connection was enlisted when the operation began but is no longer enlisted,
     * the transaction was completed during the operation and the result is unreliable.
     */
    private void endOperation(boolean wasEnlisted) throws SQLException {
        handler.readUnlock();
        if ( wasEnlisted && !handler.isEnlisted() ) {
            throw new SQLException( "Connection operation on a transaction that has been completed" );
        }
    }
    
    @Override
    public void close() throws SQLException {
        try {
            wrappedResultSet.close();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            wrappedResultSet = CLOSED_RESULT_SET;
            pruneClosed();
            statement.pruneClosedResultSets();
        }
    }

    // --- //

    @Override
    public boolean next() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.next();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.wasNull();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getString( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getBoolean( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getByte( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getShort( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getInt( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getLong( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getFloat( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getDouble( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getBigDecimal( columnIndex, scale );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getBytes( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getDate( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getTime( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getTimestamp( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getAsciiStream( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getUnicodeStream( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getBinaryStream( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getString( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getBoolean( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getByte( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getShort( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getInt( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getLong( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getFloat( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getDouble( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getBigDecimal( columnLabel, scale );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getBytes( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getDate( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getTime( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getTimestamp( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getAsciiStream( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getUnicodeStream( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getBinaryStream( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getWarnings();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.clearWarnings();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public String getCursorName() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getCursorName();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getMetaData();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getObject( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getObject( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.findColumn( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getCharacterStream( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getCharacterStream( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getBigDecimal( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getBigDecimal( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.isBeforeFirst();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.isAfterLast();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public boolean isFirst() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.isFirst();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public boolean isLast() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.isLast();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void beforeFirst() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.beforeFirst();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void afterLast() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.afterLast();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public boolean first() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.first();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public boolean last() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.last();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public int getRow() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getRow();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.absolute( row );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.relative( rows );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public boolean previous() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.previous();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getFetchDirection();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.setFetchDirection( direction );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getFetchSize();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.setFetchSize( rows );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public int getType() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getType();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public int getConcurrency() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getConcurrency();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.rowUpdated();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public boolean rowInserted() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.rowInserted();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.rowDeleted();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateNull( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBoolean( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateByte( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateShort( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateInt( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateLong( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateFloat( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateDouble( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBigDecimal( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateString( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBytes( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateDate( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateTime( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateTimestamp( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateAsciiStream( columnIndex, x, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBinaryStream( columnIndex, x, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateCharacterStream( columnIndex, x, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateObject( columnIndex, x, scaleOrLength );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateObject( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateObject( columnLabel, x, scaleOrLength );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateObject( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateNull( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBoolean( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateByte( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateShort( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateInt( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateLong( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateFloat( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateDouble( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBigDecimal( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateString( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBytes( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateDate( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateTime( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateTimestamp( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateAsciiStream( columnLabel, x, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBinaryStream( columnLabel, x, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateCharacterStream( columnLabel, reader, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void insertRow() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.insertRow();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateRow() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateRow();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void deleteRow() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.deleteRow();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void refreshRow() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.refreshRow();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.cancelRowUpdates();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.moveToInsertRow();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.moveToCurrentRow();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getObject( columnIndex, map );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getRef( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getBlob( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getClob( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getArray( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getObject( columnLabel, map );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getRef( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getBlob( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getClob( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getArray( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getDate( columnIndex, cal );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getDate( columnLabel, cal );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getTime( columnIndex, cal );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getTime( columnLabel, cal );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getTimestamp( columnIndex, cal );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getTimestamp( columnLabel, cal );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getURL( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getURL( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateRef( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateRef( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBlob( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBlob( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateClob( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateClob( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateArray( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateArray( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getRowId( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getRowId( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateRowId( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateRowId( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getHoldability();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        try {
            return wrappedResultSet.isClosed();
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }            
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateNString( columnIndex, nString );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateNString( columnLabel, nString );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateNClob( columnIndex, nClob );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateNClob( columnLabel, nClob );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getNClob( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getNClob( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getSQLXML( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getSQLXML( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateSQLXML( columnIndex, xmlObject );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateSQLXML( columnLabel, xmlObject );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getNString( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getNString( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getNCharacterStream( columnIndex );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getNCharacterStream( columnLabel );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateNCharacterStream( columnIndex, x, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateNCharacterStream( columnLabel, reader, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateAsciiStream( columnIndex, x, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBinaryStream( columnIndex, x, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateCharacterStream( columnIndex, x, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateAsciiStream( columnLabel, x, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBinaryStream( columnLabel, x, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateCharacterStream( columnLabel, reader, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBlob( columnIndex, inputStream, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBlob( columnLabel, inputStream, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateClob( columnIndex, reader, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateClob( columnLabel, reader, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateNClob( columnIndex, reader, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateClob( columnLabel, reader, length );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateNCharacterStream( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateNCharacterStream( columnLabel, reader );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateAsciiStream( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBinaryStream( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateCharacterStream( columnIndex, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateAsciiStream( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBinaryStream( columnLabel, x );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateCharacterStream( columnLabel, reader );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBlob( columnIndex, inputStream );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateBlob( columnLabel, inputStream );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateClob( columnIndex, reader );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateClob( columnLabel, reader );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateNClob( columnIndex, reader );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateNClob( columnLabel, reader );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getObject( columnIndex, type );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedResultSet.getObject( columnLabel, type );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    // --- JDBC 4.2 //

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength)  throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateObject( columnIndex, x, targetSqlType, scaleOrLength );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateObject( columnLabel, x, targetSqlType, scaleOrLength );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateObject( columnIndex, x, targetSqlType );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedResultSet.updateObject( columnLabel, x, targetSqlType );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    // --- //

    @Override
    public <T> T unwrap(Class<T> target) throws SQLException {
        try {
            return wrappedResultSet.unwrap( target );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }            
    }

    @Override
    public boolean isWrapperFor(Class<?> target) throws SQLException {
        try {
            return wrappedResultSet.isWrapperFor( target );
        } catch ( SQLException se ) {
            handler.setFlushOnly( se );
            throw se;
        }            
    }

    @Override
    protected boolean internalClosed() {
        return wrappedResultSet == CLOSED_RESULT_SET;
    }
}
