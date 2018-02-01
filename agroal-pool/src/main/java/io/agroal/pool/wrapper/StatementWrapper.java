// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper;

import io.agroal.pool.util.StampedCopyOnWriteArrayList;

import java.lang.reflect.InvocationHandler;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collection;

import static java.lang.reflect.Proxy.newProxyInstance;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class StatementWrapper implements Statement {

    private static final InvocationHandler CLOSED_HANDLER = (proxy, method, args) -> {
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
    };

    private static final Statement CLOSED_STATEMENT = (Statement) newProxyInstance( Statement.class.getClassLoader(), new Class[]{Statement.class}, CLOSED_HANDLER );

    // --- //

    private final Collection<ResultSet> trackedResultSets = new StampedCopyOnWriteArrayList<>( ResultSet.class );

    private final ConnectionWrapper connection;

    private Statement wrappedStatement;

    public StatementWrapper(ConnectionWrapper connectionWrapper, Statement statement) {
        connection = connectionWrapper;
        wrappedStatement = statement;
    }

    // --- //

    protected ResultSet trackResultSet(ResultSet resultSet) {
        if ( resultSet == null ) {
            return null;
        }
        ResultSet wrappedResultSet = new ResultSetWrapper( this, resultSet );
        trackedResultSets.add( wrappedResultSet );
        return wrappedResultSet;
    }

    protected void closeTrackedResultSets() throws SQLException {
        if ( !trackedResultSets.isEmpty() ) {
            for ( ResultSet resultSet : trackedResultSets ) {
                resultSet.close();
            }
            trackedResultSets.clear();
        }
    }

    public void releaseTrackedResultSet(ResultSet resultSet) {
        trackedResultSets.remove( resultSet );
    }

    @Override
    public void close() throws SQLException {
        connection.releaseTrackedStatement( wrappedStatement );
        wrappedStatement = CLOSED_STATEMENT;
        closeTrackedResultSets();
    }

    // --- //

    @Override
    public final void clearWarnings() throws SQLException {
        wrappedStatement.clearWarnings();
    }

    @Override
    public final ResultSet executeQuery(String sql) throws SQLException {
        return trackResultSet( wrappedStatement.executeQuery( sql ) );
    }

    @Override
    public final int executeUpdate(String sql) throws SQLException {
        return wrappedStatement.executeUpdate( sql );
    }

    @Override
    public final int getMaxFieldSize() throws SQLException {
        return wrappedStatement.getMaxFieldSize();
    }

    @Override
    public final void setMaxFieldSize(int max) throws SQLException {
        wrappedStatement.setMaxFieldSize( max );
    }

    @Override
    public final int getMaxRows() throws SQLException {
        return wrappedStatement.getMaxRows();
    }

    @Override
    public final void setMaxRows(int max) throws SQLException {
        wrappedStatement.setMaxRows( max );
    }

    @Override
    public final void setEscapeProcessing(boolean enable) throws SQLException {
        wrappedStatement.setEscapeProcessing( enable );
    }

    @Override
    public final int getQueryTimeout() throws SQLException {
        return wrappedStatement.getQueryTimeout();
    }

    @Override
    public final void setQueryTimeout(int seconds) throws SQLException {
        wrappedStatement.setQueryTimeout( seconds );
    }

    @Override
    public final void cancel() throws SQLException {
        wrappedStatement.cancel();
    }

    @Override
    public final void setCursorName(String name) throws SQLException {
        wrappedStatement.setCursorName( name );
    }

    @Override
    public final boolean execute(String sql) throws SQLException {
        return wrappedStatement.execute( sql );
    }

    @Override
    public final ResultSet getResultSet() throws SQLException {
        return trackResultSet( wrappedStatement.getResultSet() );
    }

    @Override
    public final int getUpdateCount() throws SQLException {
        return wrappedStatement.getUpdateCount();
    }

    @Override
    public final boolean getMoreResults() throws SQLException {
        return wrappedStatement.getMoreResults();
    }

    @Override
    public final int getFetchDirection() throws SQLException {
        return wrappedStatement.getFetchDirection();
    }

    @Override
    public final void setFetchDirection(int direction) throws SQLException {
        wrappedStatement.setFetchDirection( direction );
    }

    @Override
    public final int getFetchSize() throws SQLException {
        return wrappedStatement.getFetchSize();
    }

    @Override
    public final void setFetchSize(int rows) throws SQLException {
        wrappedStatement.setFetchSize( rows );
    }

    @Override
    public final int getResultSetConcurrency() throws SQLException {
        return wrappedStatement.getResultSetConcurrency();
    }

    @Override
    public final int getResultSetType() throws SQLException {
        return wrappedStatement.getResultSetType();
    }

    @Override
    public final void addBatch(String sql) throws SQLException {
        wrappedStatement.addBatch( sql );
    }

    @Override
    public final void clearBatch() throws SQLException {
        wrappedStatement.clearBatch();
    }

    @Override
    public final int[] executeBatch() throws SQLException {
        return wrappedStatement.executeBatch();
    }

    @Override
    public final Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public final boolean getMoreResults(int current) throws SQLException {
        return wrappedStatement.getMoreResults( current );
    }

    @Override
    public final ResultSet getGeneratedKeys() throws SQLException {
        return trackResultSet( wrappedStatement.getGeneratedKeys() );
    }

    @Override
    public final int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return wrappedStatement.executeUpdate( sql, autoGeneratedKeys );
    }

    @Override
    public final int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return wrappedStatement.executeUpdate( sql, columnIndexes );
    }

    @Override
    public final int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return wrappedStatement.executeUpdate( sql, columnNames );
    }

    @Override
    public final boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return wrappedStatement.execute( sql, autoGeneratedKeys );
    }

    @Override
    public final boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return wrappedStatement.execute( sql, columnIndexes );
    }

    @Override
    public final boolean execute(String sql, String[] columnNames) throws SQLException {
        return wrappedStatement.execute( sql, columnNames );
    }

    @Override
    public final int getResultSetHoldability() throws SQLException {
        return wrappedStatement.getResultSetHoldability();
    }

    @Override
    public final boolean isPoolable() throws SQLException {
        return wrappedStatement.isPoolable();
    }

    @Override
    public final void setPoolable(boolean poolable) throws SQLException {
        wrappedStatement.setPoolable( poolable );
    }

    @Override
    public final void closeOnCompletion() throws SQLException {
        wrappedStatement.closeOnCompletion();
    }

    @Override
    public final boolean isCloseOnCompletion() throws SQLException {
        return wrappedStatement.isCloseOnCompletion();
    }

    @Override
    public final SQLWarning getWarnings() throws SQLException {
        return wrappedStatement.getWarnings();
    }

    @Override
    public final boolean isClosed() throws SQLException {
        return wrappedStatement.isClosed();
    }

    // --- //

    @Override
    public final <T> T unwrap(Class<T> target) throws SQLException {
        return wrappedStatement.unwrap( target );
    }

    @Override
    public final boolean isWrapperFor(Class<?> target) throws SQLException {
        return wrappedStatement.isWrapperFor( target );
    }

    @Override
    public final String toString() {
        return "wrapped[ " + wrappedStatement + " ]";
    }
}
