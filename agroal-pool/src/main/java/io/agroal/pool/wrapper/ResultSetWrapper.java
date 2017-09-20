// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.InvocationHandler;
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
 */
public class ResultSetWrapper implements ResultSet {

    private static final InvocationHandler CLOSED_HANDLER = (proxy, method, args) -> {
        switch ( method.getName() ) {
            case "close":
                return Void.TYPE;
            case "isClosed":
                return Boolean.TRUE;
            case "toString":
                return ResultSetWrapper.class.getSimpleName() + ".CLOSED_STATEMENT";
            default:
                throw new SQLException( "ResultSet is closed" );
        }
    };

    private static final ResultSet CLOSED_RESULT_SET = (ResultSet) newProxyInstance( ResultSet.class.getClassLoader(), new Class[]{ResultSet.class}, CLOSED_HANDLER );

    // --- //

    private final StatementWrapper statement;

    private ResultSet wrappedResultSet;

    public ResultSetWrapper(StatementWrapper statementWrapper, ResultSet resultSet) {
        statement = statementWrapper;
        wrappedResultSet = resultSet;
    }

    @Override
    public void close() throws SQLException {
        statement.releaseTrackedResultSet( wrappedResultSet );
        wrappedResultSet = CLOSED_RESULT_SET;
    }

    // --- //

    @Override
    public boolean next() throws SQLException {
        return false;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wrappedResultSet.wasNull();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return wrappedResultSet.getString( columnIndex );
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return wrappedResultSet.getBoolean( columnIndex );
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return wrappedResultSet.getByte( columnIndex );
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return wrappedResultSet.getShort( columnIndex );
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return wrappedResultSet.getInt( columnIndex );
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return wrappedResultSet.getLong( columnIndex );
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return wrappedResultSet.getFloat( columnIndex );
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return wrappedResultSet.getDouble( columnIndex );
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return wrappedResultSet.getBigDecimal( columnIndex, scale );
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return wrappedResultSet.getBytes( columnIndex );
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return wrappedResultSet.getDate( columnIndex );
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return wrappedResultSet.getTime( columnIndex );
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return wrappedResultSet.getTimestamp( columnIndex );
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return wrappedResultSet.getAsciiStream( columnIndex );
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return wrappedResultSet.getUnicodeStream( columnIndex );
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return wrappedResultSet.getBinaryStream( columnIndex );
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return wrappedResultSet.getString( columnLabel );
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return wrappedResultSet.getBoolean( columnLabel );
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return wrappedResultSet.getByte( columnLabel );
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return wrappedResultSet.getShort( columnLabel );
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return wrappedResultSet.getInt( columnLabel );
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return wrappedResultSet.getLong( columnLabel );
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return wrappedResultSet.getFloat( columnLabel );
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return wrappedResultSet.getDouble( columnLabel );
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return wrappedResultSet.getBigDecimal( columnLabel, scale );
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return wrappedResultSet.getBytes( columnLabel );
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return wrappedResultSet.getDate( columnLabel );
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return wrappedResultSet.getTime( columnLabel );
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return wrappedResultSet.getTimestamp( columnLabel );
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return wrappedResultSet.getAsciiStream( columnLabel );
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return wrappedResultSet.getUnicodeStream( columnLabel );
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return wrappedResultSet.getBinaryStream( columnLabel );
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return wrappedResultSet.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        wrappedResultSet.clearWarnings();
    }

    @Override
    public String getCursorName() throws SQLException {
        return wrappedResultSet.getCursorName();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return wrappedResultSet.getMetaData();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return wrappedResultSet.getObject( columnIndex );
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return wrappedResultSet.getObject( columnLabel );
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return wrappedResultSet.findColumn( columnLabel );
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return wrappedResultSet.getCharacterStream( columnIndex );
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return wrappedResultSet.getCharacterStream( columnLabel );
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return wrappedResultSet.getBigDecimal( columnIndex );
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return wrappedResultSet.getBigDecimal( columnLabel );
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return wrappedResultSet.isBeforeFirst();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return wrappedResultSet.isAfterLast();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return wrappedResultSet.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        return wrappedResultSet.isLast();
    }

    @Override
    public void beforeFirst() throws SQLException {
        wrappedResultSet.beforeFirst();
    }

    @Override
    public void afterLast() throws SQLException {
        wrappedResultSet.afterLast();
    }

    @Override
    public boolean first() throws SQLException {
        return wrappedResultSet.first();
    }

    @Override
    public boolean last() throws SQLException {
        return wrappedResultSet.last();
    }

    @Override
    public int getRow() throws SQLException {
        return wrappedResultSet.getRow();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return wrappedResultSet.absolute( row );
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return wrappedResultSet.relative( rows );
    }

    @Override
    public boolean previous() throws SQLException {
        return wrappedResultSet.previous();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return wrappedResultSet.getFetchDirection();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        wrappedResultSet.setFetchDirection( direction );
    }

    @Override
    public int getFetchSize() throws SQLException {
        return wrappedResultSet.getFetchSize();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        wrappedResultSet.setFetchDirection( rows );
    }

    @Override
    public int getType() throws SQLException {
        return wrappedResultSet.getType();
    }

    @Override
    public int getConcurrency() throws SQLException {
        return wrappedResultSet.getConcurrency();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return wrappedResultSet.rowUpdated();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return wrappedResultSet.rowInserted();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return wrappedResultSet.rowDeleted();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        wrappedResultSet.updateNull( columnIndex );
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        wrappedResultSet.updateBoolean( columnIndex, x );
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        wrappedResultSet.updateByte( columnIndex, x );
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        wrappedResultSet.updateShort( columnIndex, x );
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        wrappedResultSet.updateInt( columnIndex, x );
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        wrappedResultSet.updateLong( columnIndex, x );
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        wrappedResultSet.updateFloat( columnIndex, x );
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        wrappedResultSet.updateDouble( columnIndex, x );
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        wrappedResultSet.updateBigDecimal( columnIndex, x );
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        wrappedResultSet.updateString( columnIndex, x );
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        wrappedResultSet.updateBytes( columnIndex, x );
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        wrappedResultSet.updateDate( columnIndex, x );
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        wrappedResultSet.updateTime( columnIndex, x );
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        wrappedResultSet.updateTimestamp( columnIndex, x );
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        wrappedResultSet.updateAsciiStream( columnIndex, x, length );
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        wrappedResultSet.updateBinaryStream( columnIndex, x, length );
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        wrappedResultSet.updateCharacterStream( columnIndex, x, length );
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        wrappedResultSet.updateObject( columnIndex, x, scaleOrLength );
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        wrappedResultSet.updateObject( columnIndex, x );
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        wrappedResultSet.updateNull( columnLabel );
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        wrappedResultSet.updateBoolean( columnLabel, x );
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        wrappedResultSet.updateByte( columnLabel, x );
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        wrappedResultSet.updateShort( columnLabel, x );
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        wrappedResultSet.updateInt( columnLabel, x );
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        wrappedResultSet.updateLong( columnLabel, x );
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        wrappedResultSet.updateFloat( columnLabel, x );
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        wrappedResultSet.updateDouble( columnLabel, x );
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        wrappedResultSet.updateBigDecimal( columnLabel, x );
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        wrappedResultSet.updateString( columnLabel, x );
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        wrappedResultSet.updateBytes( columnLabel, x );
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        wrappedResultSet.updateDate( columnLabel, x );
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        wrappedResultSet.updateTime( columnLabel, x );
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        wrappedResultSet.updateTimestamp( columnLabel, x );
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        wrappedResultSet.updateAsciiStream( columnLabel, x, length );
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        wrappedResultSet.updateBinaryStream( columnLabel, x, length );
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        wrappedResultSet.updateCharacterStream( columnLabel, reader, length );
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        wrappedResultSet.updateObject( columnLabel, x, scaleOrLength );
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        wrappedResultSet.updateObject( columnLabel, x );
    }

    @Override
    public void insertRow() throws SQLException {
        wrappedResultSet.insertRow();
    }

    @Override
    public void updateRow() throws SQLException {
        wrappedResultSet.updateRow();
    }

    @Override
    public void deleteRow() throws SQLException {
        wrappedResultSet.deleteRow();
    }

    @Override
    public void refreshRow() throws SQLException {
        wrappedResultSet.refreshRow();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        wrappedResultSet.cancelRowUpdates();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        wrappedResultSet.moveToInsertRow();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        wrappedResultSet.moveToCurrentRow();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return wrappedResultSet.getObject( columnIndex, map );
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return wrappedResultSet.getRef( columnIndex );
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return wrappedResultSet.getBlob( columnIndex );
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return wrappedResultSet.getClob( columnIndex );
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return wrappedResultSet.getArray( columnIndex );
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return wrappedResultSet.getObject( columnLabel, map );
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return wrappedResultSet.getRef( columnLabel );
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return wrappedResultSet.getBlob( columnLabel );
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return wrappedResultSet.getClob( columnLabel );
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return wrappedResultSet.getArray( columnLabel );
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return wrappedResultSet.getDate( columnIndex, cal );
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return wrappedResultSet.getDate( columnLabel, cal );
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return wrappedResultSet.getTime( columnIndex, cal );
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return wrappedResultSet.getTime( columnLabel, cal );
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return wrappedResultSet.getTimestamp( columnIndex, cal );
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return wrappedResultSet.getTimestamp( columnLabel, cal );
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return wrappedResultSet.getURL( columnIndex );
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return wrappedResultSet.getURL( columnLabel );
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        wrappedResultSet.updateRef( columnIndex, x );
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        wrappedResultSet.updateRef( columnLabel, x );
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        wrappedResultSet.updateBlob( columnIndex, x );
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        wrappedResultSet.updateBlob( columnLabel, x );
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        wrappedResultSet.updateClob( columnIndex, x );
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        wrappedResultSet.updateClob( columnLabel, x );
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        wrappedResultSet.updateArray( columnIndex, x );
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        wrappedResultSet.updateArray( columnLabel, x );
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return wrappedResultSet.getRowId( columnIndex );
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return wrappedResultSet.getRowId( columnLabel );
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        wrappedResultSet.updateRowId( columnIndex, x );
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        wrappedResultSet.updateRowId( columnLabel, x );
    }

    @Override
    public int getHoldability() throws SQLException {
        return wrappedResultSet.getHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return wrappedResultSet.isClosed();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        wrappedResultSet.updateNString( columnIndex, nString );
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        wrappedResultSet.updateNString( columnLabel, nString );
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        wrappedResultSet.updateNClob( columnIndex, nClob );
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        wrappedResultSet.updateNClob( columnLabel, nClob );
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return wrappedResultSet.getNClob( columnIndex );
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return wrappedResultSet.getNClob( columnLabel );
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return wrappedResultSet.getSQLXML( columnIndex );
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return wrappedResultSet.getSQLXML( columnLabel );
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        wrappedResultSet.updateSQLXML( columnIndex, xmlObject );
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        wrappedResultSet.updateSQLXML( columnLabel, xmlObject );
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return wrappedResultSet.getNString( columnIndex );
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return wrappedResultSet.getNString( columnLabel );
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return wrappedResultSet.getNCharacterStream( columnIndex );
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return wrappedResultSet.getNCharacterStream( columnLabel );
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        wrappedResultSet.updateNCharacterStream( columnIndex, x, length );
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        wrappedResultSet.updateNCharacterStream( columnLabel, reader, length );
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        wrappedResultSet.updateAsciiStream( columnIndex, x, length );
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        wrappedResultSet.updateBinaryStream( columnIndex, x, length );
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        wrappedResultSet.updateCharacterStream( columnIndex, x, length );
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        wrappedResultSet.updateAsciiStream( columnLabel, x, length );
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        wrappedResultSet.updateBinaryStream( columnLabel, x, length );
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        wrappedResultSet.updateCharacterStream( columnLabel, reader, length );
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        wrappedResultSet.updateBlob( columnIndex, inputStream, length );
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        wrappedResultSet.updateBlob( columnLabel, inputStream, length );
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        wrappedResultSet.updateClob( columnIndex, reader, length );
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        wrappedResultSet.updateClob( columnLabel, reader, length );
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        wrappedResultSet.updateNClob( columnIndex, reader, length );
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
         wrappedResultSet.updateClob( columnLabel, reader, length );
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        wrappedResultSet.updateNCharacterStream( columnIndex, x );
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
       wrappedResultSet.updateNCharacterStream( columnLabel, reader );
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        wrappedResultSet.updateAsciiStream( columnIndex, x );
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
       wrappedResultSet.updateBinaryStream( columnIndex, x );
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
           wrappedResultSet.updateCharacterStream( columnIndex, x );
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
       wrappedResultSet.updateAsciiStream( columnLabel, x );
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
       wrappedResultSet.updateBinaryStream( columnLabel, x );
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
         wrappedResultSet.updateCharacterStream( columnLabel, reader );
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
              wrappedResultSet.updateBlob( columnIndex, inputStream );
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
             wrappedResultSet.updateBlob( columnLabel, inputStream );
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
                wrappedResultSet.updateClob( columnIndex, reader );
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
                   wrappedResultSet.updateClob( columnLabel, reader );
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
                   wrappedResultSet.updateNClob( columnIndex, reader );
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
                   wrappedResultSet.updateNClob( columnLabel, reader );
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return wrappedResultSet.getObject( columnIndex, type );
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return wrappedResultSet.getObject( columnLabel, type );
    }

    @Override
    public <T> T unwrap(Class<T> target) throws SQLException {
        return wrappedResultSet.unwrap( target );
    }

    @Override
    public boolean isWrapperFor(Class<?> target) throws SQLException {
        return wrappedResultSet.isWrapperFor( target );
    }
}