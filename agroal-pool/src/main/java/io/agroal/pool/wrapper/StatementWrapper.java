// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.reflect.Proxy.newProxyInstance;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class StatementWrapper implements Statement {

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
                    throw new SQLException( "Statement is closed" );
            }
        }
    };

    private static final Statement CLOSED_STATEMENT = (Statement) newProxyInstance( Statement.class.getClassLoader(), new Class[]{Statement.class}, CLOSED_HANDLER );

    // --- //

    protected final ConnectionWrapper connection;

    // Collection of ResultSet to close them on close(). If null ResultSet are not tracked.
    private final Collection<ResultSet> trackedResultSets;

    private Statement wrappedStatement;

    public StatementWrapper(ConnectionWrapper connectionWrapper, Statement statement, boolean trackResources) {
        connection = connectionWrapper;
        wrappedStatement = statement;
        trackedResultSets = trackResources ? new ConcurrentLinkedQueue<>() : null;
    }

    // --- //

    protected ResultSet trackResultSet(ResultSet resultSet) {
        if ( trackedResultSets != null && resultSet != null ) {
            ResultSet wrappedResultSet = new ResultSetWrapper( this, resultSet );
            trackedResultSets.add( wrappedResultSet );
            return wrappedResultSet;
        }
        return resultSet;
    }

    protected void closeTrackedResultSets() throws SQLException {
        if ( trackedResultSets != null && !trackedResultSets.isEmpty() ) {
            for ( ResultSet resultSet : trackedResultSets ) {
                resultSet.close();
            }
            trackedResultSets.clear();
        }
    }

    public void releaseTrackedResultSet(ResultSet resultSet) {
        if ( trackedResultSets != null ) {
            trackedResultSets.remove( resultSet );
        }
    }

    public int trackedResultSetSize() {
        return trackedResultSets != null ? trackedResultSets.size() : 0;
    }

    ConnectionWrapper getConnectionWrapper() throws SQLException {
        return connection;
    }

    @Override
    public void close() throws SQLException {
        try {
            if ( wrappedStatement != CLOSED_STATEMENT ) {
                closeTrackedResultSets();
                wrappedStatement.close();
            }
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            connection.releaseTrackedStatement( this );
            wrappedStatement = CLOSED_STATEMENT;
        }
    }

    // --- //

    @Override
    public final void clearWarnings() throws SQLException {
        try {
            wrappedStatement.clearWarnings();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final ResultSet executeQuery(String sql) throws SQLException {
        try {
            return trackResultSet( wrappedStatement.executeQuery( sql ) );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int executeUpdate(String sql) throws SQLException {
        try {
            return wrappedStatement.executeUpdate( sql );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getMaxFieldSize() throws SQLException {
        try {
            return wrappedStatement.getMaxFieldSize();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void setMaxFieldSize(int max) throws SQLException {
        try {
            wrappedStatement.setMaxFieldSize( max );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getMaxRows() throws SQLException {
        try {
            return wrappedStatement.getMaxRows();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void setMaxRows(int max) throws SQLException {
        try {
            wrappedStatement.setMaxRows( max );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void setEscapeProcessing(boolean enable) throws SQLException {
        try {
            wrappedStatement.setEscapeProcessing( enable );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getQueryTimeout() throws SQLException {
        try {
            return wrappedStatement.getQueryTimeout();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void setQueryTimeout(int seconds) throws SQLException {
        try {
            wrappedStatement.setQueryTimeout( seconds );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void cancel() throws SQLException {
        try {
            wrappedStatement.cancel();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void setCursorName(String name) throws SQLException {
        try {
            wrappedStatement.setCursorName( name );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean execute(String sql) throws SQLException {
        try {
            return wrappedStatement.execute( sql );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final ResultSet getResultSet() throws SQLException {
        try {
            return trackResultSet( wrappedStatement.getResultSet() );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getUpdateCount() throws SQLException {
        try {
            return wrappedStatement.getUpdateCount();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean getMoreResults() throws SQLException {
        try {
            return wrappedStatement.getMoreResults();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getFetchDirection() throws SQLException {
        try {
            return wrappedStatement.getFetchDirection();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void setFetchDirection(int direction) throws SQLException {
        try {
            wrappedStatement.setFetchDirection( direction );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getFetchSize() throws SQLException {
        try {
            return wrappedStatement.getFetchSize();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void setFetchSize(int rows) throws SQLException {
        try {
            wrappedStatement.setFetchSize( rows );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getResultSetConcurrency() throws SQLException {
        try {
            return wrappedStatement.getResultSetConcurrency();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getResultSetType() throws SQLException {
        try {
            return wrappedStatement.getResultSetType();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void addBatch(String sql) throws SQLException {
        try {
            wrappedStatement.addBatch( sql );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void clearBatch() throws SQLException {
        try {
            wrappedStatement.clearBatch();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int[] executeBatch() throws SQLException {
        try {
            return wrappedStatement.executeBatch();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public final boolean getMoreResults(int current) throws SQLException {
        try {
            return wrappedStatement.getMoreResults( current );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final ResultSet getGeneratedKeys() throws SQLException {
        try {
            return trackResultSet( wrappedStatement.getGeneratedKeys() );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            return wrappedStatement.executeUpdate( sql, autoGeneratedKeys );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        try {
            return wrappedStatement.executeUpdate( sql, columnIndexes );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int executeUpdate(String sql, String[] columnNames) throws SQLException {
        try {
            return wrappedStatement.executeUpdate( sql, columnNames );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            return wrappedStatement.execute( sql, autoGeneratedKeys );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean execute(String sql, int[] columnIndexes) throws SQLException {
        try {
            return wrappedStatement.execute( sql, columnIndexes );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean execute(String sql, String[] columnNames) throws SQLException {
        try {
            return wrappedStatement.execute( sql, columnNames );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final int getResultSetHoldability() throws SQLException {
        try {
            return wrappedStatement.getResultSetHoldability();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean isPoolable() throws SQLException {
        try {
            return wrappedStatement.isPoolable();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void setPoolable(boolean poolable) throws SQLException {
        try {
            wrappedStatement.setPoolable( poolable );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final void closeOnCompletion() throws SQLException {
        try {
            wrappedStatement.closeOnCompletion();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean isCloseOnCompletion() throws SQLException {
        try {
            return wrappedStatement.isCloseOnCompletion();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final SQLWarning getWarnings() throws SQLException {
        try {
            return wrappedStatement.getWarnings();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean isClosed() throws SQLException {
        try {
            return wrappedStatement.isClosed();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    // --- JDBC 4.2 //

    @Override
    public long getLargeUpdateCount() throws SQLException {
        try {
            return wrappedStatement.getLargeUpdateCount();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        try {
            wrappedStatement.setLargeMaxRows( max );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        try {
            return wrappedStatement.getLargeMaxRows();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        try {
            return wrappedStatement.executeLargeBatch();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        try {
            return wrappedStatement.executeLargeUpdate( sql );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            return wrappedStatement.executeLargeUpdate( sql, autoGeneratedKeys );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        try {
            return wrappedStatement.executeLargeUpdate( sql, columnIndexes );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        try {
            return wrappedStatement.executeLargeUpdate( sql, columnNames );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    // --- //

    @Override
    public final <T> T unwrap(Class<T> target) throws SQLException {
        try {
            return wrappedStatement.unwrap( target );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final boolean isWrapperFor(Class<?> target) throws SQLException {
        try {
            return wrappedStatement.isWrapperFor( target );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public final String toString() {
        return "wrapped[ " + wrappedStatement + " ]";
    }
}
