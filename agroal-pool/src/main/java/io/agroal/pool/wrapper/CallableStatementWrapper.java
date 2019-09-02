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
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
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

    public CallableStatementWrapper(ConnectionWrapper connectionWrapper, CallableStatement statement, boolean trackJdbcResources) {
        super( connectionWrapper, statement, trackJdbcResources );
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
        try {
            wrappedStatement.registerOutParameter( parameterIndex, sqlType );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        try {
            wrappedStatement.registerOutParameter( parameterIndex, sqlType, scale );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        try {
            return wrappedStatement.wasNull();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getString( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getBoolean( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getByte( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getShort( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getInt( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getLong( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getFloat( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getDouble( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        try {
            return wrappedStatement.getBigDecimal( parameterIndex, scale );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getBytes( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getDate( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getTime( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getTimestamp( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getObject( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getBigDecimal( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        try {
            return wrappedStatement.getObject( parameterIndex, map );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getRef( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getBlob( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getClob( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getArray( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        try {
            return wrappedStatement.getDate( parameterIndex, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        try {
            return wrappedStatement.getTime( parameterIndex, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        try {
            return wrappedStatement.getTimestamp( parameterIndex, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        try {
            wrappedStatement.registerOutParameter( parameterIndex, sqlType, typeName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        try {
            wrappedStatement.registerOutParameter( parameterName, sqlType );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        try {
            wrappedStatement.registerOutParameter( parameterName, sqlType, scale );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        try {
            wrappedStatement.registerOutParameter( parameterName, sqlType, typeName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getURL( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        try {
            wrappedStatement.setURL( parameterName, val );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        try {
            wrappedStatement.setNull( parameterName, sqlType );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        try {
            wrappedStatement.setBoolean( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        try {
            wrappedStatement.setByte( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        try {
            wrappedStatement.setShort( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        try {
            wrappedStatement.setInt( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        try {
            wrappedStatement.setLong( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        try {
            wrappedStatement.setFloat( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        try {
            wrappedStatement.setDouble( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        try {
            wrappedStatement.setBigDecimal( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        try {
            wrappedStatement.setString( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        try {
            wrappedStatement.setBytes( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        try {
            wrappedStatement.setDate( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        try {
            wrappedStatement.setTime( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        try {
            wrappedStatement.setTimestamp( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        try {
            wrappedStatement.setAsciiStream( parameterName, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        try {
            wrappedStatement.setBinaryStream( parameterName, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        try {
            wrappedStatement.setObject( parameterName, x, targetSqlType, scale );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        try {
            wrappedStatement.setObject( parameterName, x, targetSqlType );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        try {
            wrappedStatement.setObject( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        try {
            wrappedStatement.setCharacterStream( parameterName, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        try {
            wrappedStatement.setDate( parameterName, x, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        try {
            wrappedStatement.setTime( parameterName, x, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        try {
            wrappedStatement.setTimestamp( parameterName, x, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        try {
            wrappedStatement.setNull( parameterName, sqlType, typeName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getString( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getBoolean( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getByte( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getShort( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getInt( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getLong( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getFloat( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getDouble( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getBytes( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getDate( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getTime( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getTimestamp( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getObject( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getBigDecimal( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        try {
            return wrappedStatement.getObject( parameterName, map );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getRef( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getBlob( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getClob( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getArray( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        try {
            return wrappedStatement.getDate( parameterName, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        try {
            return wrappedStatement.getTime( parameterName, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        try {
            return wrappedStatement.getTimestamp( parameterName, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getURL( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getRowId( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getRowId( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        try {
            wrappedStatement.setRowId( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        try {
            wrappedStatement.setNString( parameterName, value );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        try {
            wrappedStatement.setNCharacterStream( parameterName, value, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        try {
            wrappedStatement.setNClob( parameterName, value );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        try {
            wrappedStatement.setClob( parameterName, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        try {
            wrappedStatement.setBlob( parameterName, inputStream, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        try {
            wrappedStatement.setNClob( parameterName, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getNClob( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getNClob( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        try {
            wrappedStatement.setSQLXML( parameterName, xmlObject );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getSQLXML( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getSQLXML( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getNString( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getNString( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getNCharacterStream( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getNCharacterStream( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        try {
            return wrappedStatement.getCharacterStream( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        try {
            return wrappedStatement.getCharacterStream( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        try {
            wrappedStatement.setBlob( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        try {
            wrappedStatement.setClob( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        try {
            wrappedStatement.setAsciiStream( parameterName, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        try {
            wrappedStatement.setBinaryStream( parameterName, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        try {
            wrappedStatement.setCharacterStream( parameterName, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        try {
            wrappedStatement.setAsciiStream( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        try {
            wrappedStatement.setBinaryStream( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        try {
            wrappedStatement.setCharacterStream( parameterName, reader );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        try {
            wrappedStatement.setNCharacterStream( parameterName, value );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        try {
            wrappedStatement.setClob( parameterName, reader );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        try {
            wrappedStatement.setBlob( parameterName, inputStream );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        try {
            wrappedStatement.setClob( parameterName, reader );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        try {
            return wrappedStatement.getObject( parameterIndex, type );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        try {
            return wrappedStatement.getObject( parameterName, type );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        try {
            return trackResultSet( wrappedStatement.executeQuery() );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        try {
            return wrappedStatement.executeUpdate();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        try {
            wrappedStatement.setNull( parameterIndex, sqlType );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        try {
            wrappedStatement.setBoolean( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        try {
            wrappedStatement.setByte( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        try {
            wrappedStatement.setShort( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        try {
            wrappedStatement.setInt( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        try {
            wrappedStatement.setLong( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        try {
            wrappedStatement.setFloat( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        try {
            wrappedStatement.setDouble( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        try {
            wrappedStatement.setBigDecimal( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        try {
            wrappedStatement.setString( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        try {
            wrappedStatement.setBytes( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        try {
            wrappedStatement.setDate( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        try {
            wrappedStatement.setTime( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        try {
            wrappedStatement.setTimestamp( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            wrappedStatement.setAsciiStream( parameterIndex, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            wrappedStatement.setUnicodeStream( parameterIndex, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            wrappedStatement.setBinaryStream( parameterIndex, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        try {
            wrappedStatement.clearParameters();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        try {
            wrappedStatement.setObject( parameterIndex, x, targetSqlType );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        try {
            wrappedStatement.setObject( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public boolean execute() throws SQLException {
        try {
            return wrappedStatement.execute();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void addBatch() throws SQLException {
        try {
            wrappedStatement.addBatch();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        try {
            wrappedStatement.setCharacterStream( parameterIndex, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        try {
            wrappedStatement.setRef( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        try {
            wrappedStatement.setBlob( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        try {
            wrappedStatement.setClob( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        try {
            wrappedStatement.setArray( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            return wrappedStatement.getMetaData();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        try {
            wrappedStatement.setDate( parameterIndex, x, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        try {
            wrappedStatement.setTime( parameterIndex, x, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        try {
            wrappedStatement.setTimestamp( parameterIndex, x, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        try {
            wrappedStatement.setNull( parameterIndex, sqlType, typeName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        try {
            wrappedStatement.setURL( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        try {
            return wrappedStatement.getParameterMetaData();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        try {
            wrappedStatement.setRowId( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        try {
            wrappedStatement.setNString( parameterIndex, value );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        try {
            wrappedStatement.setNCharacterStream( parameterIndex, value, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        try {
            wrappedStatement.setNClob( parameterIndex, value );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            wrappedStatement.setClob( parameterIndex, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        try {
            wrappedStatement.setBlob( parameterIndex, inputStream, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            wrappedStatement.setNClob( parameterIndex, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        try {
            wrappedStatement.setSQLXML( parameterIndex, xmlObject );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        try {
            wrappedStatement.setObject( parameterIndex, x, targetSqlType, scaleOrLength );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            wrappedStatement.setAsciiStream( parameterIndex, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            wrappedStatement.setBinaryStream( parameterIndex, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            wrappedStatement.setCharacterStream( parameterIndex, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            wrappedStatement.setAsciiStream( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            wrappedStatement.setBinaryStream( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        try {
            wrappedStatement.setCharacterStream( parameterIndex, reader );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        try {
            wrappedStatement.setNCharacterStream( parameterIndex, value );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            wrappedStatement.setClob( parameterIndex, reader );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        try {
            wrappedStatement.setBlob( parameterIndex, inputStream );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            wrappedStatement.setNClob( parameterIndex, reader );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }
}
