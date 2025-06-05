// Copyright (C) 2025 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import io.agroal.pool.util.AutoCloseableElement;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class BaseStatementWrapper<T extends Statement> extends AutoCloseableElement implements Statement {

    static final class BaseStatementWrapperState<S extends Statement> {
        final S wrappedStatement;
        final AutoCloseableElement trackedResultSets;
        final ConnectionWrapper connection;

        // --- //

        BaseStatementWrapperState(ConnectionWrapper connection, S wrappedStatement, AutoCloseableElement trackedResultSets) {
            this.connection = connection;
            this.trackedResultSets = trackedResultSets;
            this.wrappedStatement = wrappedStatement;
        }
    }

    // --- //

    protected BaseStatementWrapperState<T> state;

    public BaseStatementWrapper(ConnectionWrapper connectionWrapper, T statement, boolean trackResources, AutoCloseableElement head) {
        super( head );
        state = new BaseStatementWrapperState<T>(connectionWrapper, statement, trackResources ? AutoCloseableElement.newHead() : null);
    }

    // --- //

    protected ResultSet trackResultSet(ResultSet resultSet) {
        if ( state.trackedResultSets != null && resultSet != null ) {
            return new ResultSetWrapper( this, resultSet, state.trackedResultSets );
        }
        return resultSet;
    }

    private void closeTrackedResultSets() throws SQLException {
        if ( state.trackedResultSets != null ) {
            state.connection.addLeakedResultSets( state.trackedResultSets.closeAllAutocloseableElements() );
        }
    }

    ConnectionWrapper getConnectionWrapper() throws SQLException {
        return state.connection;
    }

    @Override
    public void close() throws SQLException {
        try {
            if ( state.wrappedStatement != getClosedStatement() ) {
                closeTrackedResultSets();
                state.wrappedStatement.close();
            }
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            state = state.connection.getClosedStatementState(this);
        }
    }

    protected T getClosedStatement() {
        throw new UnsupportedOperationException("getClosedStatement() must be overridden in a subclass");
    }

    // --- //

    @Override
    public final void clearWarnings() throws SQLException {
        try {
            state.wrappedStatement.clearWarnings();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final ResultSet executeQuery(String sql) throws SQLException {
        try {
            return trackResultSet( state.wrappedStatement.executeQuery( sql ) );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int executeUpdate(String sql) throws SQLException {
        try {
            return state.wrappedStatement.executeUpdate( sql );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getMaxFieldSize() throws SQLException {
        try {
            return state.wrappedStatement.getMaxFieldSize();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void setMaxFieldSize(int max) throws SQLException {
        try {
            state.wrappedStatement.setMaxFieldSize( max );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getMaxRows() throws SQLException {
        try {
            return state.wrappedStatement.getMaxRows();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void setMaxRows(int max) throws SQLException {
        try {
            state.wrappedStatement.setMaxRows( max );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void setEscapeProcessing(boolean enable) throws SQLException {
        try {
            state.wrappedStatement.setEscapeProcessing( enable );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getQueryTimeout() throws SQLException {
        try {
            return state.wrappedStatement.getQueryTimeout();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void setQueryTimeout(int seconds) throws SQLException {
        try {
            state.wrappedStatement.setQueryTimeout( seconds );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void cancel() throws SQLException {
        try {
            state.wrappedStatement.cancel();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void setCursorName(String name) throws SQLException {
        try {
            state.wrappedStatement.setCursorName( name );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean execute(String sql) throws SQLException {
        try {
            return state.wrappedStatement.execute( sql );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final ResultSet getResultSet() throws SQLException {
        try {
            return trackResultSet( state.wrappedStatement.getResultSet() );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getUpdateCount() throws SQLException {
        try {
            return state.wrappedStatement.getUpdateCount();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean getMoreResults() throws SQLException {
        try {
            return state.wrappedStatement.getMoreResults();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getFetchDirection() throws SQLException {
        try {
            return state.wrappedStatement.getFetchDirection();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void setFetchDirection(int direction) throws SQLException {
        try {
            state.wrappedStatement.setFetchDirection( direction );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getFetchSize() throws SQLException {
        try {
            return state.wrappedStatement.getFetchSize();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void setFetchSize(int rows) throws SQLException {
        try {
            state.wrappedStatement.setFetchSize( rows );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getResultSetConcurrency() throws SQLException {
        try {
            return state.wrappedStatement.getResultSetConcurrency();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getResultSetType() throws SQLException {
        try {
            return state.wrappedStatement.getResultSetType();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void addBatch(String sql) throws SQLException {
        try {
            state.wrappedStatement.addBatch( sql );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void clearBatch() throws SQLException {
        try {
            state.wrappedStatement.clearBatch();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int[] executeBatch() throws SQLException {
        try {
            return state.wrappedStatement.executeBatch();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final Connection getConnection() throws SQLException {
        return state.connection;
    }

    @Override
    public final boolean getMoreResults(int current) throws SQLException {
        try {
            return state.wrappedStatement.getMoreResults( current );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final ResultSet getGeneratedKeys() throws SQLException {
        try {
            return trackResultSet( state.wrappedStatement.getGeneratedKeys() );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            return state.wrappedStatement.executeUpdate( sql, autoGeneratedKeys );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        try {
            return state.wrappedStatement.executeUpdate( sql, columnIndexes );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int executeUpdate(String sql, String[] columnNames) throws SQLException {
        try {
            return state.wrappedStatement.executeUpdate( sql, columnNames );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            return state.wrappedStatement.execute( sql, autoGeneratedKeys );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean execute(String sql, int[] columnIndexes) throws SQLException {
        try {
            return state.wrappedStatement.execute( sql, columnIndexes );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean execute(String sql, String[] columnNames) throws SQLException {
        try {
            return state.wrappedStatement.execute( sql, columnNames );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getResultSetHoldability() throws SQLException {
        try {
            return state.wrappedStatement.getResultSetHoldability();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean isPoolable() throws SQLException {
        try {
            return state.wrappedStatement.isPoolable();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void setPoolable(boolean poolable) throws SQLException {
        try {
            state.wrappedStatement.setPoolable( poolable );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void closeOnCompletion() throws SQLException {
        try {
            state.wrappedStatement.closeOnCompletion();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean isCloseOnCompletion() throws SQLException {
        try {
            return state.wrappedStatement.isCloseOnCompletion();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final SQLWarning getWarnings() throws SQLException {
        try {
            return state.wrappedStatement.getWarnings();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean isClosed() throws SQLException {
        try {
            return state.wrappedStatement.isClosed();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    // --- JDBC 4.2 //

    @Override
    public long getLargeUpdateCount() throws SQLException {
        try {
            return state.wrappedStatement.getLargeUpdateCount();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        try {
            return state.wrappedStatement.getLargeMaxRows();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        try {
            state.wrappedStatement.setLargeMaxRows( max );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        try {
            return state.wrappedStatement.executeLargeBatch();
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        try {
            return state.wrappedStatement.executeLargeUpdate( sql );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            return state.wrappedStatement.executeLargeUpdate( sql, autoGeneratedKeys );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        try {
            return state.wrappedStatement.executeLargeUpdate( sql, columnIndexes );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        try {
            return state.wrappedStatement.executeLargeUpdate( sql, columnNames );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    // --- //

    @Override
    public final <T> T unwrap(Class<T> target) throws SQLException {
        try {
            return state.wrappedStatement.unwrap( target );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean isWrapperFor(Class<?> target) throws SQLException {
        try {
            return state.wrappedStatement.isWrapperFor( target );
        } catch ( SQLException se ) {
            state.connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final String toString() {
        return "wrapped[ " + state.wrappedStatement + " ]";
    }
}
