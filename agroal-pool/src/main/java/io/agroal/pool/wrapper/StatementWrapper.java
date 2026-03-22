// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper;

import io.agroal.pool.util.AutoCloseableElement;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import static java.lang.reflect.Proxy.newProxyInstance;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class StatementWrapper extends AutoCloseableElement<StatementWrapper> implements Statement {

    static final String CLOSED_STATEMENT_STRING = StatementWrapper.class.getSimpleName() + ".CLOSED_STATEMENT";

    private static final InvocationHandler CLOSED_HANDLER = new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch ( method.getName() ) {
                case "close":
                    return Void.TYPE;
                case "isClosed":
                    return Boolean.TRUE;
                case "toString":
                    return CLOSED_STATEMENT_STRING;
                default:
                    throw new SQLException( "Statement is closed" );
            }
        }
    };

    private static final Statement CLOSED_STATEMENT = (Statement) newProxyInstance( Statement.class.getClassLoader(), new Class[]{Statement.class}, CLOSED_HANDLER );

    // --- //

    @SuppressWarnings( "ProtectedField" )
    protected final ConnectionWrapper connection;

    // Collection of ResultSet to close them on close(). If null ResultSet are not tracked.
    private final AutoCloseableElement<ResultSetWrapper> trackedResultSets;

    // tracks the current holdability state of this statement
    private final boolean holdState;

    // tracks the state of closeOnCompletion
    private boolean closeOnCompletionState;

    private Statement wrappedStatement;

    public StatementWrapper(ConnectionWrapper connectionWrapper, Statement statement, boolean trackResources, AutoCloseableElement<StatementWrapper> head, boolean defaultHold) {
        super( head );
        connection = connectionWrapper;
        wrappedStatement = statement;
        trackedResultSets = trackResources ? newHead() : null;
        holdState = defaultHold;
    }

    // --- //

    protected void verifyEnlistment() throws SQLException {
        if ( holdState ) {
            connection.verifyEnlistment();
        }
    }

    /**
     * Acquires the read lock and verifies enlistment. Must be called before the try block so that
     * endOperation is only reached when the lock was successfully acquired. Returns the enlisted state
     * to be passed to {@link #endOperation(boolean)}.
     */
    protected boolean beginOperation() throws SQLException {
        connection.getHandler().readLock();
        boolean wasEnlisted = connection.getHandler().isEnlisted();
        try {
            connection.verifyEnlistment();
        } catch ( SQLException se ) {
            connection.getHandler().readUnlock();
            throw se;
        }
        return wasEnlisted;
    }

    /**
     * Releases the read lock after a statement operation completes.
     * If the connection was enlisted when the operation began but is no longer enlisted,
     * the transaction was completed during the operation and the result is unreliable.
     */
    protected void endOperation(boolean wasEnlisted) throws SQLException {
        connection.getHandler().readUnlock();
        if ( wasEnlisted && !connection.getHandler().isEnlisted() ) {
            throw new SQLException( "Connection operation on a transaction that has been completed" );
        }
    }

    protected ResultSet trackResultSet(ResultSet resultSet) {
        if ( trackedResultSets != null && resultSet != null ) {
            return new ResultSetWrapper( this, resultSet, trackedResultSets, holdState );
        }
        return resultSet;
    }

    ConnectionWrapper getConnectionWrapper() {
        return connection;
    }

    @Override
    public void close() throws SQLException {
        try {
            if ( wrappedStatement != CLOSED_STATEMENT ) {
                if ( trackedResultSets != null ) {
                    connection.addLeakedResultSets( trackedResultSets.closeAllAutocloseableElements() );
                }
                wrappedStatement.close();
            }
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            wrappedStatement = CLOSED_STATEMENT;
            pruneClosed();
            connection.pruneClosedStatements();
        }
    }

    // --- //

    @Override
    public final void clearWarnings() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedStatement.clearWarnings();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final ResultSet executeQuery(String sql) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return trackResultSet( wrappedStatement.executeQuery( sql ) );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final int executeUpdate(String sql) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.executeUpdate( sql );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final int getMaxFieldSize() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.getMaxFieldSize();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final void setMaxFieldSize(int max) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedStatement.setMaxFieldSize( max );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final int getMaxRows() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.getMaxRows();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final void setMaxRows(int max) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedStatement.setMaxRows( max );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final void setEscapeProcessing(boolean enable) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedStatement.setEscapeProcessing( enable );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final int getQueryTimeout() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.getQueryTimeout();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final void setQueryTimeout(int seconds) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedStatement.setQueryTimeout( seconds );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final void cancel() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedStatement.cancel();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final void setCursorName(String name) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedStatement.setCursorName( name );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final boolean execute(String sql) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.execute( sql );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final ResultSet getResultSet() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return trackResultSet( wrappedStatement.getResultSet() );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final int getUpdateCount() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.getUpdateCount();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final boolean getMoreResults() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.getMoreResults();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final int getFetchDirection() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.getFetchDirection();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final void setFetchDirection(int direction) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedStatement.setFetchDirection( direction );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final int getFetchSize() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.getFetchSize();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final void setFetchSize(int rows) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedStatement.setFetchSize( rows );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final int getResultSetConcurrency() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.getResultSetConcurrency();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final int getResultSetType() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.getResultSetType();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final void addBatch(String sql) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedStatement.addBatch( sql );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final void clearBatch() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedStatement.clearBatch();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final int[] executeBatch() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.executeBatch();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final Connection getConnection() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return connection;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final boolean getMoreResults(int current) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.getMoreResults( current );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final ResultSet getGeneratedKeys() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return trackResultSet( wrappedStatement.getGeneratedKeys() );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.executeUpdate( sql, autoGeneratedKeys );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.executeUpdate( sql, columnIndexes );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final int executeUpdate(String sql, String[] columnNames) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.executeUpdate( sql, columnNames );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.execute( sql, autoGeneratedKeys );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final boolean execute(String sql, int[] columnIndexes) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.execute( sql, columnIndexes );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final boolean execute(String sql, String[] columnNames) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.execute( sql, columnNames );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final int getResultSetHoldability() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.getResultSetHoldability();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final boolean isPoolable() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.isPoolable();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final void setPoolable(boolean poolable) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedStatement.setPoolable( poolable );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final void closeOnCompletion() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedStatement.closeOnCompletion();
            closeOnCompletionState = true;
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final boolean isCloseOnCompletion() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.isCloseOnCompletion();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public final SQLWarning getWarnings() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.getWarnings();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
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
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.getLargeUpdateCount();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.getLargeMaxRows();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            wrappedStatement.setLargeMaxRows( max );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.executeLargeBatch();
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.executeLargeUpdate( sql );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.executeLargeUpdate( sql, autoGeneratedKeys );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.executeLargeUpdate( sql, columnIndexes );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
        }
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        boolean enlisted = beginOperation();
        try {
            return wrappedStatement.executeLargeUpdate( sql, columnNames );
        } catch ( SQLException se ) {
            connection.getHandler().setFlushOnly( se );
            throw se;
        } finally {
            endOperation( enlisted );
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

    void pruneClosedResultSets() {
        if ( trackedResultSets != null ) {
            trackedResultSets.pruneClosed();
            if  ( closeOnCompletionState && trackedResultSets.isElementListEmpty() ) {
                try {
                    close();
                } catch ( SQLException se ) {
                    // ignore
                }
            }
        }
    }

    @Override
    public boolean isHeld() {
        return holdState;
    }

    @Override
    protected void beforeClose() {
        try {
            cancel(); // AG-231 - we have to cancel the Statement on cleanup to avoid overloading the DB
        } catch ( SQLException e ) {
            // ignore and proceed with close()
        }
    }

    @Override
    protected boolean internalClosed() {
        return wrappedStatement == CLOSED_STATEMENT;
    }
}
