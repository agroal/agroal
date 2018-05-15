// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import static java.lang.reflect.Proxy.newProxyInstance;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class CallableStatementWrapper extends StatementWrapper implements CallableStatement {

    private static final InvocationHandler CLOSED_HANDLER = new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch ( method.getName() ) {
                case "close":
                    return Void.TYPE;
                case "isClosed":
                    return Boolean.TRUE;
                case "toString":
                    return StatementWrapper.class.getSimpleName() + ".CLOSED_STATEMENT";
                default:
                    throw new SQLException( "CallableStatement is closed" );
            }
        }
    };

    private static final CallableStatement CLOSED_STATEMENT = (CallableStatement) newProxyInstance( CallableStatement.class.getClassLoader(), new Class[]{CallableStatement.class}, CLOSED_HANDLER );

    // --- //

    private CallableStatement wrappedStatement;

    public CallableStatementWrapper(ConnectionWrapper connectionWrapper, CallableStatement statement) {
        super( connectionWrapper, statement );
        wrappedStatement = statement;
    }

    @Override
    public void close() throws SQLException {
        wrappedStatement = CLOSED_STATEMENT;
        super.close();
    }

    // --- //

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        wrappedStatement.registerOutParameter( parameterIndex, sqlType );
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        wrappedStatement.registerOutParameter( parameterIndex, sqlType, scale );
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wrappedStatement.wasNull();
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        return wrappedStatement.getString( parameterIndex );
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        return wrappedStatement.getBoolean( parameterIndex );
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        return wrappedStatement.getByte( parameterIndex );
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        return wrappedStatement.getShort( parameterIndex );
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        return wrappedStatement.getInt( parameterIndex );
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        return wrappedStatement.getLong( parameterIndex );
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        return wrappedStatement.getFloat( parameterIndex );
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        return wrappedStatement.getDouble( parameterIndex );
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return wrappedStatement.getBigDecimal( parameterIndex, scale );
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        return wrappedStatement.getBytes( parameterIndex );
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        return wrappedStatement.getDate( parameterIndex );
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        return wrappedStatement.getTime( parameterIndex );
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return wrappedStatement.getTimestamp( parameterIndex );
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        return wrappedStatement.getObject( parameterIndex );
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return wrappedStatement.getBigDecimal( parameterIndex );
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        return wrappedStatement.getObject( parameterIndex, map );
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        return wrappedStatement.getRef( parameterIndex );
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        return wrappedStatement.getBlob( parameterIndex );
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        return wrappedStatement.getClob( parameterIndex );
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        return wrappedStatement.getArray( parameterIndex );
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return wrappedStatement.getDate( parameterIndex, cal );
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return wrappedStatement.getTime( parameterIndex, cal );
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return wrappedStatement.getTimestamp( parameterIndex, cal );
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        wrappedStatement.registerOutParameter( parameterIndex, sqlType, typeName );
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        wrappedStatement.registerOutParameter( parameterName, sqlType );
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        wrappedStatement.registerOutParameter( parameterName, sqlType, scale );
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        wrappedStatement.registerOutParameter( parameterName, sqlType, typeName );
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        return wrappedStatement.getURL( parameterIndex );
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        wrappedStatement.setURL( parameterName, val );
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        wrappedStatement.setNull( parameterName, sqlType );
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        wrappedStatement.setBoolean( parameterName, x );
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        wrappedStatement.setByte( parameterName, x );
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        wrappedStatement.setShort( parameterName, x );
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        wrappedStatement.setInt( parameterName, x );
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        wrappedStatement.setLong( parameterName, x );
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        wrappedStatement.setFloat( parameterName, x );
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        wrappedStatement.setDouble( parameterName, x );
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        wrappedStatement.setBigDecimal( parameterName, x );
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        wrappedStatement.setString( parameterName, x );
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        wrappedStatement.setBytes( parameterName, x );
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        wrappedStatement.setDate( parameterName, x );
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        wrappedStatement.setTime( parameterName, x );
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        wrappedStatement.setTimestamp( parameterName, x );
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        wrappedStatement.setAsciiStream( parameterName, x, length );
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        wrappedStatement.setBinaryStream( parameterName, x, length );
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        wrappedStatement.setObject( parameterName, x, targetSqlType, scale );
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        wrappedStatement.setObject( parameterName, x, targetSqlType );
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        wrappedStatement.setObject( parameterName, x );
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        wrappedStatement.setCharacterStream( parameterName, reader, length );
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        wrappedStatement.setDate( parameterName, x, cal );
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        wrappedStatement.setTime( parameterName, x, cal );
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        wrappedStatement.setTimestamp( parameterName, x, cal );
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        wrappedStatement.setNull( parameterName, sqlType, typeName );
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        return wrappedStatement.getString( parameterName );
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        return wrappedStatement.getBoolean( parameterName );
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        return wrappedStatement.getByte( parameterName );
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        return wrappedStatement.getShort( parameterName );
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        return wrappedStatement.getInt( parameterName );
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        return wrappedStatement.getLong( parameterName );
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        return wrappedStatement.getFloat( parameterName );
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        return wrappedStatement.getDouble( parameterName );
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        return wrappedStatement.getBytes( parameterName );
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        return wrappedStatement.getDate( parameterName );
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        return wrappedStatement.getTime( parameterName );
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return wrappedStatement.getTimestamp( parameterName );
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        return wrappedStatement.getObject( parameterName );
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return wrappedStatement.getBigDecimal( parameterName );
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return wrappedStatement.getObject( parameterName, map );
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        return wrappedStatement.getRef( parameterName );
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        return wrappedStatement.getBlob( parameterName );
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        return wrappedStatement.getClob( parameterName );
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        return wrappedStatement.getArray( parameterName );
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return wrappedStatement.getDate( parameterName, cal );
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return wrappedStatement.getTime( parameterName, cal );
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return wrappedStatement.getTimestamp( parameterName, cal );
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        return wrappedStatement.getURL( parameterName );
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        return wrappedStatement.getRowId( parameterIndex );
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        return wrappedStatement.getRowId( parameterName );
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        wrappedStatement.setRowId( parameterName, x );
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        wrappedStatement.setNString( parameterName, value );
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        wrappedStatement.setNCharacterStream( parameterName, value, length );
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        wrappedStatement.setNClob( parameterName, value );
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        wrappedStatement.setClob( parameterName, reader, length );
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        wrappedStatement.setBlob( parameterName, inputStream, length );
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        wrappedStatement.setNClob( parameterName, reader, length );
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        return wrappedStatement.getNClob( parameterIndex );
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        return wrappedStatement.getNClob( parameterName );
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        wrappedStatement.setSQLXML( parameterName, xmlObject );
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return wrappedStatement.getSQLXML( parameterIndex );
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return wrappedStatement.getSQLXML( parameterName );
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        return wrappedStatement.getNString( parameterIndex );
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        return wrappedStatement.getNString( parameterName );
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return wrappedStatement.getNCharacterStream( parameterIndex );
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return wrappedStatement.getNCharacterStream( parameterName );
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return wrappedStatement.getCharacterStream( parameterIndex );
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        return wrappedStatement.getCharacterStream( parameterName );
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        wrappedStatement.setBlob( parameterName, x );
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        wrappedStatement.setClob( parameterName, x );
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        wrappedStatement.setAsciiStream( parameterName, x, length );
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        wrappedStatement.setBinaryStream( parameterName, x, length );
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        wrappedStatement.setCharacterStream( parameterName, reader, length );
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        wrappedStatement.setAsciiStream( parameterName, x );
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        wrappedStatement.setBinaryStream( parameterName, x );
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        wrappedStatement.setCharacterStream( parameterName, reader );
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        wrappedStatement.setNCharacterStream( parameterName, value );
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        wrappedStatement.setClob( parameterName, reader );
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        wrappedStatement.setBlob( parameterName, inputStream );
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        wrappedStatement.setClob( parameterName, reader );
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        return wrappedStatement.getObject( parameterIndex, type );
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        return wrappedStatement.getObject( parameterName, type );
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return trackResultSet( wrappedStatement.executeQuery() );
    }

    @Override
    public int executeUpdate() throws SQLException {
        return wrappedStatement.executeUpdate();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        wrappedStatement.setNull( parameterIndex, sqlType );
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        wrappedStatement.setBoolean( parameterIndex, x );
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        wrappedStatement.setByte( parameterIndex, x );
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        wrappedStatement.setShort( parameterIndex, x );
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        wrappedStatement.setInt( parameterIndex, x );
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        wrappedStatement.setLong( parameterIndex, x );
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        wrappedStatement.setFloat( parameterIndex, x );
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        wrappedStatement.setDouble( parameterIndex, x );
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        wrappedStatement.setBigDecimal( parameterIndex, x );
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        wrappedStatement.setString( parameterIndex, x );
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        wrappedStatement.setBytes( parameterIndex, x );
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        wrappedStatement.setDate( parameterIndex, x );
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        wrappedStatement.setTime( parameterIndex, x );
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        wrappedStatement.setTimestamp( parameterIndex, x );
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        wrappedStatement.setAsciiStream( parameterIndex, x, length );
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        wrappedStatement.setUnicodeStream( parameterIndex, x, length );
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        wrappedStatement.setBinaryStream( parameterIndex, x, length );
    }

    @Override
    public void clearParameters() throws SQLException {
        wrappedStatement.clearParameters();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        wrappedStatement.setObject( parameterIndex, x, targetSqlType );
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        wrappedStatement.setObject( parameterIndex, x );
    }

    @Override
    public boolean execute() throws SQLException {
        return wrappedStatement.execute();
    }

    @Override
    public void addBatch() throws SQLException {
        wrappedStatement.addBatch();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        wrappedStatement.setCharacterStream( parameterIndex, reader, length );
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        wrappedStatement.setRef( parameterIndex, x );
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        wrappedStatement.setBlob( parameterIndex, x );
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        wrappedStatement.setClob( parameterIndex, x );
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        wrappedStatement.setArray( parameterIndex, x );
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return wrappedStatement.getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        wrappedStatement.setDate( parameterIndex, x, cal );
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        wrappedStatement.setTime( parameterIndex, x, cal );
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        wrappedStatement.setTimestamp( parameterIndex, x, cal );
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        wrappedStatement.setNull( parameterIndex, sqlType, typeName );
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        wrappedStatement.setURL( parameterIndex, x );
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return wrappedStatement.getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        wrappedStatement.setRowId( parameterIndex, x );
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        wrappedStatement.setNString( parameterIndex, value );
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        wrappedStatement.setNCharacterStream( parameterIndex, value, length );
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        wrappedStatement.setNClob( parameterIndex, value );
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        wrappedStatement.setClob( parameterIndex, reader, length );
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        wrappedStatement.setBlob( parameterIndex, inputStream, length );
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        wrappedStatement.setNClob( parameterIndex, reader, length );
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        wrappedStatement.setSQLXML( parameterIndex, xmlObject );
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        wrappedStatement.setObject( parameterIndex, x, targetSqlType, scaleOrLength );
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        wrappedStatement.setAsciiStream( parameterIndex, x, length );
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        wrappedStatement.setBinaryStream( parameterIndex, x, length );
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        wrappedStatement.setCharacterStream( parameterIndex, reader, length );
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        wrappedStatement.setAsciiStream( parameterIndex, x );
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        wrappedStatement.setBinaryStream( parameterIndex, x );
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        wrappedStatement.setCharacterStream( parameterIndex, reader );
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        wrappedStatement.setNCharacterStream( parameterIndex, value );
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        wrappedStatement.setClob( parameterIndex, reader );
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        wrappedStatement.setBlob( parameterIndex, inputStream );
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        wrappedStatement.setNClob( parameterIndex, reader );
    }
}
