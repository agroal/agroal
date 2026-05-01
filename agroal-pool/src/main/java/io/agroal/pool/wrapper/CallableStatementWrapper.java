// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper;

import io.agroal.pool.util.AutoCloseableElement;
import io.agroal.pool.wrapper.closed.ClosedCallableStatement;

import java.io.InputStream;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
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
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
@SuppressWarnings( "resource" )
public final class CallableStatementWrapper extends StatementWrapper implements CallableStatement {

    private static final VarHandle WRAPPED;

    static {
        try {
            WRAPPED = MethodHandles.lookup().findVarHandle( CallableStatementWrapper.class, "wrappedStatement", CallableStatement.class );
        } catch ( NoSuchFieldException | IllegalAccessException e ) {
            throw new ExceptionInInitializerError( e );
        }
    }

    private CallableStatement wrappedCallableStatement() {
        return (CallableStatement) WRAPPED.getAcquire( this );
    }

    // --- //

    @SuppressWarnings( "unused" )
    private CallableStatement wrappedStatement;

    public CallableStatementWrapper(ConnectionWrapper connectionWrapper, CallableStatement statement, boolean trackJdbcResources, AutoCloseableElement<StatementWrapper> head, boolean defaultHoldability) {
        super( connectionWrapper, statement, trackJdbcResources, head, defaultHoldability );
        WRAPPED.setRelease( this, statement );
    }

    @Override
    public void close() throws SQLException {
        WRAPPED.setRelease( this, ClosedCallableStatement.INSTANCE );
        super.close();
    }

    // --- //

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().registerOutParameter( parameterIndex, sqlType );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().registerOutParameter( parameterIndex, sqlType, scale );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().wasNull();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getString( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getBoolean( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getByte( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getShort( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getInt( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getLong( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getFloat( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getDouble( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getBigDecimal( parameterIndex, scale );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getBytes( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getDate( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getTime( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getTimestamp( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            Object driverObject = wrappedCallableStatement().getObject( parameterIndex );
            return driverObject instanceof ResultSet rs ? trackResultSet( rs ) : driverObject;
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getBigDecimal( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        try {
            verifyEnlistment();
            Object driverObject = wrappedCallableStatement().getObject( parameterIndex, map );
            return driverObject instanceof ResultSet rs ? trackResultSet( rs ) : driverObject;
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getRef( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getBlob( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getClob( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getArray( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getDate( parameterIndex, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getTime( parameterIndex, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getTimestamp( parameterIndex, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().registerOutParameter( parameterIndex, sqlType, typeName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().registerOutParameter( parameterName, sqlType );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().registerOutParameter( parameterName, sqlType, scale );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().registerOutParameter( parameterName, sqlType, typeName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getURL( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setURL( parameterName, val );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setNull( parameterName, sqlType );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBoolean( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setByte( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setShort( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setInt( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setLong( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setFloat( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setDouble( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBigDecimal( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setString( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBytes( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setDate( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setTime( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setTimestamp( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setAsciiStream( parameterName, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBinaryStream( parameterName, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setObject( parameterName, x, targetSqlType, scale );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setObject( parameterName, x, targetSqlType );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setObject( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setCharacterStream( parameterName, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setDate( parameterName, x, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setTime( parameterName, x, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setTimestamp( parameterName, x, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setNull( parameterName, sqlType, typeName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getString( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getBoolean( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getByte( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getShort( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getInt( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getLong( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getFloat( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getDouble( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getBytes( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getDate( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getTime( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getTimestamp( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            Object driverObject = wrappedCallableStatement().getObject( parameterName );
            return driverObject instanceof ResultSet rs ? trackResultSet( rs ) : driverObject;
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getBigDecimal( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        try {
            verifyEnlistment();
            Object driverObject = wrappedCallableStatement().getObject( parameterName, map );
            return driverObject instanceof ResultSet rs ? trackResultSet( rs ) : driverObject;
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getRef( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getBlob( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getClob( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getArray( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getDate( parameterName, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getTime( parameterName, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getTimestamp( parameterName, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getURL( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getRowId( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getRowId( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setRowId( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setNString( parameterName, value );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setNCharacterStream( parameterName, value, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setNClob( parameterName, value );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setClob( parameterName, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBlob( parameterName, inputStream, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setNClob( parameterName, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getNClob( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getNClob( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setSQLXML( parameterName, xmlObject );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getSQLXML( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getSQLXML( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getNString( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getNString( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getNCharacterStream( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getNCharacterStream( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getCharacterStream( parameterIndex );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getCharacterStream( parameterName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBlob( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setClob( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setAsciiStream( parameterName, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBinaryStream( parameterName, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setCharacterStream( parameterName, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setAsciiStream( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBinaryStream( parameterName, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setCharacterStream( parameterName, reader );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setNCharacterStream( parameterName, value );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setClob( parameterName, reader );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBlob( parameterName, inputStream );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setClob( parameterName, reader );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        try {
            verifyEnlistment();
            T driverObject = wrappedCallableStatement().getObject( parameterIndex, type );
            return driverObject instanceof ResultSet rs ? (T) trackResultSet( rs ) : driverObject;
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        try {
            verifyEnlistment();
            T driverObject = wrappedCallableStatement().getObject( parameterName, type );
            return driverObject instanceof ResultSet rs ? (T) trackResultSet( rs ) : driverObject;
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        try {
            verifyEnlistment();
            return trackResultSet( wrappedCallableStatement().executeQuery() );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().executeUpdate();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setNull( parameterIndex, sqlType );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBoolean( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setByte( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setShort( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setInt( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setLong( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setFloat( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setDouble( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBigDecimal( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setString( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBytes( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setDate( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setTime( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setTimestamp( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setAsciiStream( parameterIndex, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setUnicodeStream( parameterIndex, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBinaryStream( parameterIndex, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().clearParameters();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setObject( parameterIndex, x, targetSqlType );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setObject( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public boolean execute() throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().execute();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void addBatch() throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().addBatch();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setCharacterStream( parameterIndex, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setRef( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBlob( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setClob( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setArray( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getMetaData();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setDate( parameterIndex, x, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setTime( parameterIndex, x, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setTimestamp( parameterIndex, x, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setNull( parameterIndex, sqlType, typeName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setURL( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().getParameterMetaData();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setRowId( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setNString( parameterIndex, value );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setNCharacterStream( parameterIndex, value, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setNClob( parameterIndex, value );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setClob( parameterIndex, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBlob( parameterIndex, inputStream, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setNClob( parameterIndex, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setSQLXML( parameterIndex, xmlObject );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setObject( parameterIndex, x, targetSqlType, scaleOrLength );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setAsciiStream( parameterIndex, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBinaryStream( parameterIndex, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setCharacterStream( parameterIndex, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setAsciiStream( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBinaryStream( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setCharacterStream( parameterIndex, reader );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setNCharacterStream( parameterIndex, value );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setClob( parameterIndex, reader );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setBlob( parameterIndex, inputStream );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setNClob( parameterIndex, reader );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    // --- JDBC 4.2 //

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setObject( parameterIndex, x, targetSqlType, scaleOrLength );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        try {
            verifyEnlistment();
            wrappedCallableStatement().setObject( parameterIndex, x, targetSqlType );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        try {
            verifyEnlistment();
            return wrappedCallableStatement().executeLargeUpdate();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

}
