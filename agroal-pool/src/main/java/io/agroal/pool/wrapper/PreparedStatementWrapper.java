// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper;

import io.agroal.pool.util.AutoCloseableElement;

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

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public final class PreparedStatementWrapper extends StatementWrapper implements PreparedStatement {

    static final String CLOSED_PREPARED_STATEMENT_STRING = PreparedStatementWrapper.class.getSimpleName() + ".CLOSED_STATEMENT";

    private static final PreparedStatement CLOSED_STATEMENT = new ClosedPreparedStatement();

    // --- //

    private PreparedStatement wrappedStatement;

    public PreparedStatementWrapper(ConnectionWrapper connectionWrapper, PreparedStatement statement, boolean trackJdbcResources, AutoCloseableElement head, boolean defaultHoldability) {
        super( connectionWrapper, statement, trackJdbcResources, head, defaultHoldability );
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
        try {
            verifyEnlistment();
            return trackResultSet( wrappedStatement.executeQuery() );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        try {
            verifyEnlistment();
            return wrappedStatement.executeUpdate();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setNull( parameterIndex, sqlType );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setBoolean( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setByte( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setShort( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setInt( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setLong( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setFloat( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setDouble( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setBigDecimal( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setString( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setBytes( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setDate( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setTime( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setTimestamp( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            verifyEnlistment();
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
            verifyEnlistment();
            wrappedStatement.setUnicodeStream( parameterIndex, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setBinaryStream( parameterIndex, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.clearParameters();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setObject( parameterIndex, x, targetSqlType );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setObject( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public boolean execute() throws SQLException {
        try {
            verifyEnlistment();
            return wrappedStatement.execute();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void addBatch() throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.addBatch();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setCharacterStream( parameterIndex, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setRef( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setBlob( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setClob( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setArray( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            verifyEnlistment();
            return wrappedStatement.getMetaData();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setDate( parameterIndex, x, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setTime( parameterIndex, x, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setTimestamp( parameterIndex, x, cal );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setNull( parameterIndex, sqlType, typeName );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setURL( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        try {
            verifyEnlistment();
            return wrappedStatement.getParameterMetaData();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setRowId( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setNString( parameterIndex, value );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setNCharacterStream( parameterIndex, value, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setNClob( parameterIndex, value );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setClob( parameterIndex, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setBlob( parameterIndex, inputStream, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setNClob( parameterIndex, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setSQLXML( parameterIndex, xmlObject );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setObject( parameterIndex, x, targetSqlType, scaleOrLength );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setAsciiStream( parameterIndex, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setBinaryStream( parameterIndex, x, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setCharacterStream( parameterIndex, reader, length );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setAsciiStream( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setBinaryStream( parameterIndex, x );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setCharacterStream( parameterIndex, reader );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setNCharacterStream( parameterIndex, value );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setClob( parameterIndex, reader );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setBlob( parameterIndex, inputStream );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setNClob( parameterIndex, reader );
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
            wrappedStatement.setObject( parameterIndex, x, targetSqlType, scaleOrLength );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        try {
            verifyEnlistment();
            wrappedStatement.setObject( parameterIndex, x, targetSqlType );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        try {
            verifyEnlistment();
            return wrappedStatement.executeLargeUpdate();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    static class ClosedPreparedStatement extends StatementWrapper.ClosedStatement implements PreparedStatement {

        @Override
        protected SQLException closed() {
            return new SQLException( "PreparedStatement is closed" );
        }

        // --- PreparedStatement methods ---

        @Override
        public ResultSet executeQuery() throws SQLException { throw closed(); }

        @Override
        public int executeUpdate() throws SQLException { throw closed(); }

        @Override
        public void setNull(int parameterIndex, int sqlType) throws SQLException { throw closed(); }

        @Override
        public void setBoolean(int parameterIndex, boolean x) throws SQLException { throw closed(); }

        @Override
        public void setByte(int parameterIndex, byte x) throws SQLException { throw closed(); }

        @Override
        public void setShort(int parameterIndex, short x) throws SQLException { throw closed(); }

        @Override
        public void setInt(int parameterIndex, int x) throws SQLException { throw closed(); }

        @Override
        public void setLong(int parameterIndex, long x) throws SQLException { throw closed(); }

        @Override
        public void setFloat(int parameterIndex, float x) throws SQLException { throw closed(); }

        @Override
        public void setDouble(int parameterIndex, double x) throws SQLException { throw closed(); }

        @Override
        public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException { throw closed(); }

        @Override
        public void setString(int parameterIndex, String x) throws SQLException { throw closed(); }

        @Override
        public void setBytes(int parameterIndex, byte[] x) throws SQLException { throw closed(); }

        @Override
        public void setDate(int parameterIndex, Date x) throws SQLException { throw closed(); }

        @Override
        public void setTime(int parameterIndex, Time x) throws SQLException { throw closed(); }

        @Override
        public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException { throw closed(); }

        @Override
        public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException { throw closed(); }

        @Override
        @SuppressWarnings( "deprecation" )
        public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException { throw closed(); }

        @Override
        public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException { throw closed(); }

        @Override
        public void clearParameters() throws SQLException { throw closed(); }

        @Override
        public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException { throw closed(); }

        @Override
        public void setObject(int parameterIndex, Object x) throws SQLException { throw closed(); }

        @Override
        public boolean execute() throws SQLException { throw closed(); }

        @Override
        public void addBatch() throws SQLException { throw closed(); }

        @Override
        public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException { throw closed(); }

        @Override
        public void setRef(int parameterIndex, Ref x) throws SQLException { throw closed(); }

        @Override
        public void setBlob(int parameterIndex, Blob x) throws SQLException { throw closed(); }

        @Override
        public void setClob(int parameterIndex, Clob x) throws SQLException { throw closed(); }

        @Override
        public void setArray(int parameterIndex, Array x) throws SQLException { throw closed(); }

        @Override
        public ResultSetMetaData getMetaData() throws SQLException { throw closed(); }

        @Override
        public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException { throw closed(); }

        @Override
        public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException { throw closed(); }

        @Override
        public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException { throw closed(); }

        @Override
        public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException { throw closed(); }

        @Override
        public void setURL(int parameterIndex, URL x) throws SQLException { throw closed(); }

        @Override
        public ParameterMetaData getParameterMetaData() throws SQLException { throw closed(); }

        @Override
        public void setRowId(int parameterIndex, RowId x) throws SQLException { throw closed(); }

        @Override
        public void setNString(int parameterIndex, String value) throws SQLException { throw closed(); }

        @Override
        public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException { throw closed(); }

        @Override
        public void setNClob(int parameterIndex, NClob value) throws SQLException { throw closed(); }

        @Override
        public void setClob(int parameterIndex, Reader reader, long length) throws SQLException { throw closed(); }

        @Override
        public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException { throw closed(); }

        @Override
        public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException { throw closed(); }

        @Override
        public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException { throw closed(); }

        @Override
        public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException { throw closed(); }

        @Override
        public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException { throw closed(); }

        @Override
        public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException { throw closed(); }

        @Override
        public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException { throw closed(); }

        @Override
        public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException { throw closed(); }

        @Override
        public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException { throw closed(); }

        @Override
        public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException { throw closed(); }

        @Override
        public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException { throw closed(); }

        @Override
        public void setClob(int parameterIndex, Reader reader) throws SQLException { throw closed(); }

        @Override
        public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException { throw closed(); }

        @Override
        public void setNClob(int parameterIndex, Reader reader) throws SQLException { throw closed(); }

        @Override
        public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException { throw closed(); }

        @Override
        public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException { throw closed(); }

        @Override
        public long executeLargeUpdate() throws SQLException { throw closed(); }

        @Override
        public String toString() {
            return CLOSED_PREPARED_STATEMENT_STRING;
        }
    }
}
