// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.cache.Acquirable;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.transaction.TransactionAware;
import io.agroal.pool.util.AutoCloseableElement;
import io.agroal.pool.util.UncheckedArrayList;
import io.agroal.pool.wrapper.ConnectionWrapper;
import io.agroal.pool.wrapper.XAConnectionWrapper;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static io.agroal.pool.ConnectionHandler.DirtyAttribute.AUTOCOMMIT;
import static io.agroal.pool.ConnectionHandler.DirtyAttribute.HOLDABILITY;
import static io.agroal.pool.ConnectionHandler.DirtyAttribute.READ_ONLY;
import static io.agroal.pool.ConnectionHandler.DirtyAttribute.TRANSACTION_ISOLATION;
import static io.agroal.pool.util.ListenerHelper.fireOnInfo;
import static io.agroal.pool.util.ListenerHelper.fireOnWarning;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.copyOfRange;
import static java.util.EnumSet.noneOf;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class ConnectionHandler implements TransactionAware, Acquirable {

    private static final AtomicReferenceFieldUpdater<ConnectionHandler, State> stateUpdater = newUpdater( ConnectionHandler.class, State.class, "state" );

    private static final SQLCallable<Boolean> NO_ACTIVE_TRANSACTION = () -> false;

    // --- //

    private final XAConnection xaConnection;

    // Single Connection reference from xaConnection.getConnection()
    private final Connection connection;

    // Single XAResource reference from xaConnection.getXAResource(). Can be null for no XA datasources.
    private final XAResource xaResource;

    private final Pool connectionPool;

    // attributes that need to be reset when the connection is returned
    private final Set<DirtyAttribute> dirtyAttributes = noneOf( DirtyAttribute.class );

    // collection of wrappers created while enlisted in the current transaction
    private final AutoCloseableElement<ConnectionWrapper> enlistedOpenWrappers = AutoCloseableElement.newHead();

    // Can use annotation to get (in theory) a little better performance
    // @Contended
    private volatile State state = State.NEW;

    // for leak detection (only valid for CHECKED_OUT connections)
    private Thread holdingThread;

    // Enhanced leak report
    @SuppressWarnings( "VolatileArrayField" )
    private volatile StackTraceElement[] acquisitionStackTrace;
    private StackTraceElement[] lastOperationStackTrace;
    private List<String> connectionOperations;

    // for expiration (CHECKED_IN connections) and leak detection (CHECKED_OUT connections)
    private long lastAccess;

    // flag to indicate that this the connection is enlisted to a transaction
    private boolean enlisted;

    // the default value for isolation level and connection holdability
    private final int defaultIsolationLevel;
    private final int defaultHoldability;

    // flag to indicate the connection is held
    private boolean isHeldOverCommit;

    // reference to the task that flushes this connection when it gets over it's maxLifetime
    private Future<?> maxLifetimeTask;

    // Callback set by the transaction integration layer to prevent deferred enlistment
    // If the connection is not associated with a transaction and an operation occurs within the bounds of a transaction, an SQLException is thrown
    // If there is no transaction integration this should just return false
    private SQLCallable<Boolean> transactionActiveCheck = NO_ACTIVE_TRANSACTION;

    public ConnectionHandler(XAConnection xa, Pool pool, int isolationLevel, int holdability) throws SQLException {
        xaConnection = xa;
        connection = xaConnection.getConnection();
        xaResource = xaConnection.getXAResource();

        connectionPool = pool;
        defaultIsolationLevel = isolationLevel;
        defaultHoldability = holdability;
        touch();
    }

    public XAConnectionWrapper xaConnectionWrapper() {
        boolean trackResources = connectionPool.getConfiguration().connectionFactoryConfiguration().trackJdbcResources();
        return new XAConnectionWrapper( this, xaConnection, trackResources );
    }

    public ConnectionWrapper connectionWrapper() {
        boolean trackResources = connectionPool.getConfiguration().connectionFactoryConfiguration().trackJdbcResources();
        return new ConnectionWrapper( this, trackResources, enlisted ? enlistedOpenWrappers : null, defaultHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT );
    }

    public ConnectionWrapper detachedWrapper() {
        boolean trackResources = connectionPool.getConfiguration().connectionFactoryConfiguration().trackJdbcResources();
        return new ConnectionWrapper( this, trackResources, true, defaultHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT );
    }

    @SuppressWarnings( "StringConcatenation" )
    public void onConnectionWrapperClose(ConnectionWrapper wrapper) throws SQLException {
        if ( wrapper.getLeakedStatements() > 0 || wrapper.getLeakedResultSets() > 0 ) {
            fireOnWarning( connectionPool.getListeners(), "JDBC resources leaked: " + wrapper.getLeakedStatements() + " Statements(s) and " + wrapper.getLeakedResultSets() + " ResultSet(s)" );
        }
        if ( !enlisted && !wrapper.isDetached() ) {
            isHeldOverCommit = false;
            transactionEnd();
        }
    }

    public Connection rawConnection() {
        return connection;
    }

    public XAResource getXaResource() {
        return xaResource;
    }

    @SuppressWarnings( "MagicConstant" )
    public void resetConnection() throws SQLException {
        transactionActiveCheck = NO_ACTIVE_TRANSACTION;
        if ( !dirtyAttributes.isEmpty() ) {
            try {
                if ( dirtyAttributes.contains( AUTOCOMMIT ) ) {
                    connection.setAutoCommit( connectionPool.getConfiguration().connectionFactoryConfiguration().autoCommit() );
                }
                if ( dirtyAttributes.contains( TRANSACTION_ISOLATION ) ) {
                    connection.setTransactionIsolation( defaultIsolationLevel );
                }
                if ( dirtyAttributes.contains( READ_ONLY ) ) {
                    connection.setReadOnly( connectionPool.getConfiguration().connectionFactoryConfiguration().readOnly() );
                }
                if ( dirtyAttributes.contains( HOLDABILITY ) ) {
                    connection.setHoldability( defaultHoldability );
                }
                // other attributes do not have default values in connectionFactoryConfiguration
            } catch ( SQLException se ) {
                setFlushOnly( se );
                throw se;
            } finally {
                dirtyAttributes.clear();
            }
        }

        try {
            SQLWarning warning = connection.getWarnings();
            if ( warning != null ) {
                AgroalConnectionPoolConfiguration.ExceptionSorter exceptionSorter = connectionPool.getConfiguration().exceptionSorter();
                while ( warning != null ) {
                    if ( exceptionSorter != null && exceptionSorter.isFatal( warning ) ) {
                        stateUpdater.set( this, State.FLUSH );
                    }
                    warning = warning.getNextWarning();
                }
                connection.clearWarnings();
            }
        } catch ( SQLException se ) {
            setFlushOnly( se );
            throw se;
            // keep errors
        }
    }

    public void closeConnection() throws SQLException {
        if ( maxLifetimeTask != null && !maxLifetimeTask.isDone() ) {
            maxLifetimeTask.cancel( false );
        }
        maxLifetimeTask = null;
        try {
            State observedState = stateUpdater.get( this );
            if ( observedState != State.FLUSH ) {
                throw new SQLException( "Closing connection in incorrect state " + observedState );
            }
        } finally {
            try {
                xaConnection.close();
            } finally {
                stateUpdater.lazySet( this, State.DESTROYED );
            }
        }
    }

    // --- //

    public void markAvailable() {
        stateUpdater.lazySet( this, State.CHECKED_IN );
    }

    public boolean release() {
        return changeState( State.CHECKED_OUT, State.CHECKED_IN );
    }

    public boolean acquire() {
        return changeState( State.CHECKED_IN, State.CHECKED_OUT );
    }

    public boolean isAcquirable() {
        State observedState = stateUpdater.get( this );
        return observedState != State.FLUSH && observedState != State.DESTROYED;
    }

    private boolean isActive() {
        State observedState = stateUpdater.get( this );
        return observedState == State.CHECKED_OUT || observedState == State.FLUSH;
    }

    public boolean tryFlushFromIdle() {
        return changeState( State.CHECKED_IN, State.FLUSH );
    }

    public boolean tryFlushFromActive() {
        return changeState( State.CHECKED_OUT, State.FLUSH );
    }

    public boolean tryValidationFromIdle() {
        return changeState( State.CHECKED_IN, State.VALIDATION );
    }

    public boolean tryValidationFromActive() {
        return changeState( State.CHECKED_OUT, State.VALIDATION );
    }

    public boolean passValidationToIdle() {
        return changeState( State.VALIDATION, State.CHECKED_IN );
    }

    public boolean passValidationToActive() {
        return changeState( State.VALIDATION, State.CHECKED_OUT );
    }

    public void failValidation() {
        changeState( State.VALIDATION, State.FLUSH );
    }

    private boolean changeState(State expected, State newState) {
        if ( expected == State.DESTROYED ) {
            throw new IllegalArgumentException( "Trying to move out of state DESTROYED" );
        }

        switch ( newState ) {
            case NEW:
                throw new IllegalArgumentException( "Trying to set invalid state NEW" );
            case CHECKED_IN:
            case CHECKED_OUT:
            case VALIDATION:
            case FLUSH:
            case DESTROYED:
                return stateUpdater.compareAndSet( this, expected, newState );
            default:
                throw new IllegalArgumentException( "Trying to set invalid state " + newState );
        }
    }

    // --- //

    public void touch() {
        lastAccess = nanoTime();
    }

    public boolean isLeak(Duration timeout) {
        return isActive() && !enlisted && isIdle( timeout );
    }

    public boolean isIdle(Duration timeout) {
        return nanoTime() - lastAccess > timeout.toNanos();
    }

    public void setMaxLifetimeTask(Future<?> maxLifetimeTask) {
        this.maxLifetimeTask = maxLifetimeTask;
    }

    public boolean isValid() {
        try {
            return connectionPool.getConfiguration().connectionValidator().isValid( detachedWrapper() );
        } catch ( Throwable t ) {
            fireOnWarning( connectionPool.getListeners(), t );
            return false;
        }
    }

    public boolean isHeldOverCommit() {
        return isHeldOverCommit;
    }

    public void verifyReadOnly(boolean readOnly) throws SQLException {
        if ( enlisted ) {
            throw new SQLException( "Attempted to modify read-only state while enlisted in transaction" );
        }
        if ( connectionPool.getConfiguration().connectionFactoryConfiguration().readOnly() && !readOnly ) {
            throw new SQLException( "Attempted to modify read-only state of read-only connection" );
        }
    }

    // --- Leak detection //

    public Thread getHoldingThread() {
        return holdingThread;
    }

    public void setHoldingThread(Thread holdingThread) {
        this.holdingThread = holdingThread;
    }

    // --- Enhanced leak report //     

    /**
     * Abbreviated list of all operation on the connection, for enhanced leak report
     */
    @SuppressWarnings( "VariableNotUsedInsideIf" )
    public void traceConnectionOperation(String operation) {
        if ( acquisitionStackTrace != null ) {
            connectionOperations.add( operation );
            lastOperationStackTrace = currentThread().getStackTrace();
        }
    }

    /**
     * Abbreviated list of all operation on the connection, for enhanced leak report
     */
    public List<String> getConnectionOperations() {
        return connectionOperations;
    }

    /**
     * Stack trace of the first acquisition for this connection
     */
    public StackTraceElement[] getAcquisitionStackTrace() {
        return acquisitionStackTrace == null ? null : copyOfRange( acquisitionStackTrace, 4, acquisitionStackTrace.length );
    }

    /**
     * Stores a stack trace for leak report. Setting a value != null also enables tracing of operations on the connection
     */
    public void setAcquisitionStackTrace(StackTraceElement[] stackTrace) {
        lastOperationStackTrace = null;
        if ( connectionOperations == null ) {
            connectionOperations = new UncheckedArrayList<>( String.class );
        }
        connectionOperations.clear();
        acquisitionStackTrace = stackTrace;
    }

    /**
     * Stack trace for the last operation on this connection
     */
    public StackTraceElement[] getLastOperationStackTrace() {
        return lastOperationStackTrace == null ? null : copyOfRange( lastOperationStackTrace, 3, lastOperationStackTrace.length );
    }

    public void setDirtyAttribute(DirtyAttribute attribute) {
        dirtyAttributes.add( attribute );
    }

    public boolean isEnlisted() {
        return enlisted;
    }

    // --- TransactionAware //

    @Override
    public Connection getConnection() {
        return detachedWrapper();
    }

    @Override
    public void transactionStart() throws SQLException {
        try {
            if ( !enlisted && connection.getAutoCommit() ) {
                connection.setAutoCommit( false );
                setDirtyAttribute( AUTOCOMMIT );
            }
            enlisted = true;
        } catch ( SQLException se ) {
            setFlushOnly( se );
            throw se;
        }
    }

    @Override
    @SuppressWarnings( "StringConcatenation" )
    public void transactionBeforeCompletion(boolean successful) {
        int closedResources;
        if ( successful ) {
            closedResources = enlistedOpenWrappers.closeNotHeldAutocloseableElements();
            isHeldOverCommit = !enlistedOpenWrappers.isElementListEmpty();
            if ( isHeldOverCommit && !connectionPool.getConfiguration().connectionFactoryConfiguration().trackJdbcResources() ) {
                fireOnInfo( connectionPool.getListeners(), "Holding cursors over commit without tracking JDBC resources!" );
            }
        } else {
            closedResources = enlistedOpenWrappers.closeAllAutocloseableElements();
            isHeldOverCommit = false;
        }
        if ( successful && closedResources > 0 ) {
            fireOnWarning( connectionPool.getListeners(), "Closing " + closedResources + " open connection(s) prior to commit" );
        } else {
            // AG-168 - Close without warning as Synchronization.beforeCompletion is only invoked on success. See issue for more details.
            // fireOnWarning( connectionPool.getListeners(), "Closing open connection prior to rollback" );
        }
    }

    @Override
    public void transactionCommit() throws SQLException {
        verifyEnlistment();
        try {
            connection.commit();
        } catch ( SQLException se ) {
            setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void transactionRollback() throws SQLException {
        verifyEnlistment();
        try {
            connection.rollback();
        } catch ( SQLException se ) {
            setFlushOnly( se );
            throw se;
        }
    }

    @Override
    public void transactionEnd() throws SQLException {
        enlisted = false;
        if ( !isHeldOverCommit ) {
            if ( enlistedOpenWrappers.closeAllAutocloseableElements() != 0 ) {
                // should never happen, but it's here as a safeguard to prevent double returns in all cases.
                fireOnWarning( connectionPool.getListeners(), "Closing open connection(s) on after completion" );
            }
            connectionPool.returnConnectionHandler( this );
        }
    }

    @Override
    public void transactionCheckCallback(SQLCallable<Boolean> transactionCheck) {
        transactionActiveCheck = transactionCheck;
    }

    public void verifyEnlistment() throws SQLException {
        if ( transactionActiveCheck.call() ) {
            if ( !enlisted ) {
                if ( isHeldOverCommit ) {
                    // AG-279 Hack: if was held over commit re-attach to a new tx
                    connectionPool.getConfiguration().transactionIntegration().associate( this, xaResource );
                    isHeldOverCommit = false;
                    touch();
                } else {
                    throw new SQLException( "Deferred enlistment not supported" );
                }
            }
        } else {
            if ( isHeldOverCommit ) {
                throw new SQLException( "Connection has held cursors from a previous transaction and can only be used with a new transaction" );
            }
            if ( enlisted ) {
                throw new SQLException( "Enlisted connection used without active transaction" );
            }
        }
    }

    @Override
    public void setFlushOnly() {
        // Assumed currentState == State.CHECKED_OUT (or eventually in FLUSH already)
        stateUpdater.set( this, State.FLUSH );
    }

    public void setFlushOnly(SQLException se) {
        // Assumed currentState == State.CHECKED_OUT (or eventually in FLUSH already)
        AgroalConnectionPoolConfiguration.ExceptionSorter exceptionSorter = connectionPool.getConfiguration().exceptionSorter();
        if ( exceptionSorter != null && exceptionSorter.isFatal( se ) ) {
            stateUpdater.set( this, State.FLUSH );
        }
    }

    // --- //

    private enum State {
        NEW, CHECKED_IN, CHECKED_OUT, VALIDATION, FLUSH, DESTROYED
    }

    public enum DirtyAttribute {
        AUTOCOMMIT, TRANSACTION_ISOLATION, NETWORK_TIMEOUT, SCHEMA, CATALOG, READ_ONLY, HOLDABILITY
    }
}
