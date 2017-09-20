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

import static java.lang.System.identityHashCode;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface MockResultSet extends ResultSet {

    byte[] BYTES = new byte[0];

    // --- //

    @Override
    default boolean next() throws SQLException {
        return false;
    }

    @Override
    default void close() throws SQLException {
    }

    @Override
    default boolean wasNull() throws SQLException {
        return false;
    }

    @Override
    default String getString(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default boolean getBoolean(int columnIndex) throws SQLException {
        return false;
    }

    @Override
    default byte getByte(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    default short getShort(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    default int getInt(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    default long getLong(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    default float getFloat(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    default double getDouble(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    default BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return null;
    }

    @Override
    default byte[] getBytes(int columnIndex) throws SQLException {
        return BYTES;
    }

    @Override
    default Date getDate(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default Time getTime(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default Timestamp getTimestamp(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default InputStream getAsciiStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default InputStream getBinaryStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default String getString(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default boolean getBoolean(String columnLabel) throws SQLException {
        return false;
    }

    @Override
    default byte getByte(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    default short getShort(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    default int getInt(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    default long getLong(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    default float getFloat(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    default double getDouble(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    default BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return null;
    }

    @Override
    default byte[] getBytes(String columnLabel) throws SQLException {
        return BYTES;
    }

    @Override
    default Date getDate(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default Time getTime(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default Timestamp getTimestamp(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default InputStream getAsciiStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default InputStream getBinaryStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    default void clearWarnings() throws SQLException {

    }

    @Override
    default String getCursorName() throws SQLException {
        return null;
    }

    @Override
    default ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    default Object getObject(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default Object getObject(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default int findColumn(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    default Reader getCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default Reader getCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default boolean isBeforeFirst() throws SQLException {
        return false;
    }

    @Override
    default boolean isAfterLast() throws SQLException {
        return false;
    }

    @Override
    default boolean isFirst() throws SQLException {
        return false;
    }

    @Override
    default boolean isLast() throws SQLException {
        return false;
    }

    @Override
    default void beforeFirst() throws SQLException {

    }

    @Override
    default void afterLast() throws SQLException {

    }

    @Override
    default boolean first() throws SQLException {
        return false;
    }

    @Override
    default boolean last() throws SQLException {
        return false;
    }

    @Override
    default int getRow() throws SQLException {
        return 0;
    }

    @Override
    default boolean absolute(int row) throws SQLException {
        return false;
    }

    @Override
    default boolean relative(int rows) throws SQLException {
        return false;
    }

    @Override
    default boolean previous() throws SQLException {
        return false;
    }

    @Override
    default int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }

    @Override
    default void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    default int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    default void setFetchSize(int rows) throws SQLException {

    }

    @Override
    default int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    default int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    default boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    default boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    default boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    default void updateNull(int columnIndex) throws SQLException {
    }

    @Override
    default void updateBoolean(int columnIndex, boolean x) throws SQLException {
    }

    @Override
    default void updateByte(int columnIndex, byte x) throws SQLException {
    }

    @Override
    default void updateShort(int columnIndex, short x) throws SQLException {
    }

    @Override
    default void updateInt(int columnIndex, int x) throws SQLException {
    }

    @Override
    default void updateLong(int columnIndex, long x) throws SQLException {
    }

    @Override
    default void updateFloat(int columnIndex, float x) throws SQLException {
    }

    @Override
    default void updateDouble(int columnIndex, double x) throws SQLException {
    }

    @Override
    default void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    }

    @Override
    default void updateString(int columnIndex, String x) throws SQLException {
    }

    @Override
    default void updateBytes(int columnIndex, byte[] x) throws SQLException {
    }

    @Override
    default void updateDate(int columnIndex, Date x) throws SQLException {
    }

    @Override
    default void updateTime(int columnIndex, Time x) throws SQLException {
    }

    @Override
    default void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    }

    @Override
    default void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    }

    @Override
    default void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    }

    @Override
    default void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    }

    @Override
    default void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    }

    @Override
    default void updateObject(int columnIndex, Object x) throws SQLException {
    }

    @Override
    default void updateNull(String columnLabel) throws SQLException {
    }

    @Override
    default void updateBoolean(String columnLabel, boolean x) throws SQLException {
    }

    @Override
    default void updateByte(String columnLabel, byte x) throws SQLException {
    }

    @Override
    default void updateShort(String columnLabel, short x) throws SQLException {
    }

    @Override
    default void updateInt(String columnLabel, int x) throws SQLException {

    }

    @Override
    default void updateLong(String columnLabel, long x) throws SQLException {
    }

    @Override
    default void updateFloat(String columnLabel, float x) throws SQLException {
    }

    @Override
    default void updateDouble(String columnLabel, double x) throws SQLException {
    }

    @Override
    default void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    }

    @Override
    default void updateString(String columnLabel, String x) throws SQLException {
    }

    @Override
    default void updateBytes(String columnLabel, byte[] x) throws SQLException {
    }

    @Override
    default void updateDate(String columnLabel, Date x) throws SQLException {
    }

    @Override
    default void updateTime(String columnLabel, Time x) throws SQLException {
    }

    @Override
    default void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    }

    @Override
    default void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    }

    @Override
    default void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
    }

    @Override
    default void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
    }

    @Override
    default void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    }

    @Override
    default void updateObject(String columnLabel, Object x) throws SQLException {
    }

    @Override
    default void insertRow() throws SQLException {
    }

    @Override
    default void updateRow() throws SQLException {
    }

    @Override
    default void deleteRow() throws SQLException {
    }

    @Override
    default void refreshRow() throws SQLException {
    }

    @Override
    default void cancelRowUpdates() throws SQLException {
    }

    @Override
    default void moveToInsertRow() throws SQLException {
    }

    @Override
    default void moveToCurrentRow() throws SQLException {
    }

    @Override
    default Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    default Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    default Ref getRef(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default Blob getBlob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default Clob getClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default Array getArray(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    default Ref getRef(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default Blob getBlob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default Clob getClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default Array getArray(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    default Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    default Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    default Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    default Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    default Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    default URL getURL(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default URL getURL(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default void updateRef(int columnIndex, Ref x) throws SQLException {
    }

    @Override
    default void updateRef(String columnLabel, Ref x) throws SQLException {
    }

    @Override
    default void updateBlob(int columnIndex, Blob x) throws SQLException {
    }

    @Override
    default void updateBlob(String columnLabel, Blob x) throws SQLException {
    }

    @Override
    default void updateClob(int columnIndex, Clob x) throws SQLException {
    }

    @Override
    default void updateClob(String columnLabel, Clob x) throws SQLException {
    }

    @Override
    default void updateArray(int columnIndex, Array x) throws SQLException {
    }

    @Override
    default void updateArray(String columnLabel, Array x) throws SQLException {
    }

    @Override
    default RowId getRowId(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default RowId getRowId(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default void updateRowId(int columnIndex, RowId x) throws SQLException {
    }

    @Override
    default void updateRowId(String columnLabel, RowId x) throws SQLException {
    }

    @Override
    default int getHoldability() throws SQLException {
        return CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    default boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    default void updateNString(int columnIndex, String nString) throws SQLException {
    }

    @Override
    default void updateNString(String columnLabel, String nString) throws SQLException {
    }

    @Override
    default void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    }

    @Override
    default void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    }

    @Override
    default NClob getNClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default NClob getNClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default SQLXML getSQLXML(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default SQLXML getSQLXML(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    }

    @Override
    default void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    }

    @Override
    default String getNString(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default String getNString(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default Reader getNCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    default Reader getNCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    default void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    }

    @Override
    default void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    }

    @Override
    default void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    }

    @Override
    default void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    }

    @Override
    default void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    }

    @Override
    default void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
    }

    @Override
    default void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
    }

    @Override
    default void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    }

    @Override
    default void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
    }

    @Override
    default void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
    }

    @Override
    default void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    }

    @Override
    default void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    }

    @Override
    default void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    }

    @Override
    default void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    }

    @Override
    default void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    }

    @Override
    default void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    }

    @Override
    default void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    }

    @Override
    default void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    }

    @Override
    default void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    }

    @Override
    default void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    }

    @Override
    default void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    }

    @Override
    default void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    }

    @Override
    default void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    }

    @Override
    default void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    }

    @Override
    default void updateClob(int columnIndex, Reader reader) throws SQLException {
    }

    @Override
    default void updateClob(String columnLabel, Reader reader) throws SQLException {
    }

    @Override
    default void updateNClob(int columnIndex, Reader reader) throws SQLException {
    }

    @Override
    default void updateNClob(String columnLabel, Reader reader) throws SQLException {
    }

    @Override
    default <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    default <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    default <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    default boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    // --- //

    class Empty implements MockResultSet {

        @Override
        public String toString() {
            return "MockResultSet@" + identityHashCode( this );
        }
    }
}
