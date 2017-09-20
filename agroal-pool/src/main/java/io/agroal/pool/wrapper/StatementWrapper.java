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
    public void clearWarnings() throws SQLException {
        wrappedStatement.clearWarnings();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return trackResultSet( wrappedStatement.executeQuery( sql ) );
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return wrappedStatement.executeUpdate( sql );
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return wrappedStatement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        wrappedStatement.setMaxFieldSize( max );
    }

    @Override
    public int getMaxRows() throws SQLException {
        return wrappedStatement.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        wrappedStatement.setMaxRows( max );
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        wrappedStatement.setEscapeProcessing( enable );
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return wrappedStatement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        wrappedStatement.setQueryTimeout( seconds );
    }

    @Override
    public void cancel() throws SQLException {
        wrappedStatement.cancel();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        wrappedStatement.setCursorName( name );
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return wrappedStatement.execute( sql );
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return trackResultSet( wrappedStatement.getResultSet() );
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return wrappedStatement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return wrappedStatement.getMoreResults();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return wrappedStatement.getFetchDirection();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        wrappedStatement.setFetchDirection( direction );
    }

    @Override
    public int getFetchSize() throws SQLException {
        return wrappedStatement.getFetchSize();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        wrappedStatement.setFetchSize( rows );
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return wrappedStatement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return wrappedStatement.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        wrappedStatement.addBatch( sql );
    }

    @Override
    public void clearBatch() throws SQLException {
        wrappedStatement.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return wrappedStatement.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return wrappedStatement.getMoreResults( current );
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return trackResultSet( wrappedStatement.getGeneratedKeys() );
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return wrappedStatement.executeUpdate( sql, autoGeneratedKeys );
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return wrappedStatement.executeUpdate( sql, columnIndexes );
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return wrappedStatement.executeUpdate( sql, columnNames );
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return wrappedStatement.execute( sql, autoGeneratedKeys );
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return wrappedStatement.execute( sql, columnIndexes );
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return wrappedStatement.execute( sql, columnNames );
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return wrappedStatement.getResultSetHoldability();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return wrappedStatement.isPoolable();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        wrappedStatement.setPoolable( poolable );
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        wrappedStatement.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return wrappedStatement.isCloseOnCompletion();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return wrappedStatement.getWarnings();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return wrappedStatement.isClosed();
    }

    // --- //

    @Override
    public <T> T unwrap(Class<T> target) throws SQLException {
        return wrappedStatement.unwrap( target );
    }

    @Override
    public boolean isWrapperFor(Class<?> target) throws SQLException {
        return wrappedStatement.isWrapperFor( target );
    }

    @Override
    public String toString() {
        return "wrapped[ " + wrappedStatement + " ]";
    }
}
