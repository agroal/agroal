// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test;

import java.io.InputStream;
import java.io.Reader;
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
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import static java.lang.System.identityHashCode;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface MockPreparedStatement extends MockStatement, PreparedStatement {

    @Override
    default ResultSet executeQuery() throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default int executeUpdate() throws SQLException {
        return 0;
    }

    @Override
    default void setNull(int parameterIndex, int sqlType) throws SQLException {
    }

    @Override
    default void setBoolean(int parameterIndex, boolean x) throws SQLException {
    }

    @Override
    default void setByte(int parameterIndex, byte x) throws SQLException {
    }

    @Override
    default void setShort(int parameterIndex, short x) throws SQLException {
    }

    @Override
    default void setInt(int parameterIndex, int x) throws SQLException {
    }

    @Override
    default void setLong(int parameterIndex, long x) throws SQLException {
    }

    @Override
    default void setFloat(int parameterIndex, float x) throws SQLException {
    }

    @Override
    default void setDouble(int parameterIndex, double x) throws SQLException {
    }

    @Override
    default void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    }

    @Override
    default void setString(int parameterIndex, String x) throws SQLException {
    }

    @Override
    default void setBytes(int parameterIndex, byte[] x) throws SQLException {
    }

    @Override
    default void setDate(int parameterIndex, Date x) throws SQLException {
    }

    @Override
    default void setTime(int parameterIndex, Time x) throws SQLException {
    }

    @Override
    default void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    }

    @Override
    default void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    }

    @Override
    default void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    }

    @Override
    default void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    }

    @Override
    default void clearParameters() throws SQLException {
    }

    @Override
    default void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    }

    @Override
    default void setObject(int parameterIndex, Object x) throws SQLException {
    }

    @Override
    default boolean execute() throws SQLException {
        return false;
    }

    @Override
    default void addBatch() throws SQLException {
    }

    @Override
    default void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
    }

    @Override
    default void setRef(int parameterIndex, Ref x) throws SQLException {
    }

    @Override
    default void setBlob(int parameterIndex, Blob x) throws SQLException {
    }

    @Override
    default void setClob(int parameterIndex, Clob x) throws SQLException {
    }

    @Override
    default void setArray(int parameterIndex, Array x) throws SQLException {
    }

    @Override
    default ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    default void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    }

    @Override
    default void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    }

    @Override
    default void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    }

    @Override
    default void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    }

    @Override
    default void setURL(int parameterIndex, URL x) throws SQLException {
    }

    @Override
    default ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    default void setRowId(int parameterIndex, RowId x) throws SQLException {
    }

    @Override
    default void setNString(int parameterIndex, String value) throws SQLException {
    }

    @Override
    default void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    }

    @Override
    default void setNClob(int parameterIndex, NClob value) throws SQLException {
    }

    @Override
    default void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    }

    @Override
    default void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    }

    @Override
    default void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    }

    @Override
    default void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    }

    @Override
    default void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
    }

    @Override
    default void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    }

    @Override
    default void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    }

    @Override
    default void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    }

    @Override
    default  void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    }

    @Override
    default void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    }

    @Override
    default void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    }

    @Override
    default void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    }

    @Override
    default void setClob(int parameterIndex, Reader reader) throws SQLException {
    }

    @Override
    default void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    }

    @Override
    default void setNClob(int parameterIndex, Reader reader) throws SQLException {
    }

    @Override
    default void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        PreparedStatement.super.setObject( parameterIndex, x, targetSqlType, scaleOrLength );
    }

    @Override
    default void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        PreparedStatement.super.setObject( parameterIndex, x, targetSqlType );
    }

    @Override
    default long executeLargeUpdate() throws SQLException {
        return PreparedStatement.super.executeLargeUpdate();
    }

// --- //

    class Empty implements MockPreparedStatement {

        @Override
        public String toString() {
            return "MockPreparedStatement@" + identityHashCode( this );
        }
    }
}
