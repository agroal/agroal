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
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import static java.lang.reflect.Proxy.newProxyInstance;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class PreparedStatementWrapper extends StatementWrapper implements PreparedStatement {

    private static final InvocationHandler CLOSED_HANDLER = (proxy, method, args) -> {
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
    };

    private static final PreparedStatement CLOSED_STATEMENT = (PreparedStatement) newProxyInstance( PreparedStatement.class.getClassLoader(), new Class[]{PreparedStatement.class}, CLOSED_HANDLER );

    // --- //

    private PreparedStatement wrappedStatement;

    public PreparedStatementWrapper(ConnectionWrapper connectionWrapper, PreparedStatement statement) {
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