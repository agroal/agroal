// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static io.agroal.pool.ConnectionHandler.DirtyAttribute.AUTOCOMMIT;
import static io.agroal.pool.ConnectionHandler.DirtyAttribute.TRANSACTION_ISOLATION;
import static java.util.EnumSet.noneOf;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class ConnectionHandler {

    private static final AtomicReferenceFieldUpdater<ConnectionHandler, State> stateUpdater = newUpdater( ConnectionHandler.class, State.class, "state" );

    private final Connection connection;

    //used in XA mode, otherwise null
    private final XAResource xaResource;

    private final ConnectionPool connectionPool;

    // attributes that need to be reset when the connection is returned
    private final Set<DirtyAttribute> dirtyAttributes = noneOf( DirtyAttribute.class );

    // Can use annotation to get (in theory) a little better performance
    // @Contended
    private volatile State state;

    // for leak detection (only valid for CHECKED_OUT connections)
    private Thread holdingThread;

    // for expiration (CHECKED_IN connections) and leak detection (CHECKED_OUT connections)
    private long lastAccess;

    // flag to indicate that this the connection is enlisted to a transaction
    private boolean enlisted;

    public ConnectionHandler(XAConnection xaConnection, ConnectionPool pool) throws SQLException {
        connection = xaConnection.getConnection();
        xaResource = xaConnection.getXAResource();

        connectionPool = pool;
        state = State.NEW;
        lastAccess = System.nanoTime();
    }

    public Connection getConnection() {
        return connection;
    }

    public XAResource getXaResource() {
        return xaResource;
    }

    public void returnConnection() throws SQLException {
        connectionPool.returnConnection( this );
    }

    public void resetConnection(AgroalConnectionFactoryConfiguration connectionFactoryConfiguration) throws SQLException {
        if ( !dirtyAttributes.isEmpty() ) {
            if ( dirtyAttributes.contains( AUTOCOMMIT ) ) {
                connection.setAutoCommit( connectionFactoryConfiguration.autoCommit() );
            }
            if ( dirtyAttributes.contains( TRANSACTION_ISOLATION ) ) {
                connection.setTransactionIsolation( connectionFactoryConfiguration.jdbcTransactionIsolation().level() );
            }
            // other attributes do not have default values in connectionFactoryConfiguration

            dirtyAttributes.clear();
        }
    }

    public void closeConnection() throws SQLException {
        try {
            if ( state != State.FLUSH ) {
                throw new SQLException( "Closing connection in incorrect state " + state );
            }
        } finally {
            connection.close();
        }
    }

    public boolean setState(State expected, State newState) {
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

    public void setState(State newState) {
        // Maybe could use lazySet here, but there doesn't seem to be any performance advantage
        stateUpdater.set( this, newState );
    }

    public boolean isActive() {
        return stateUpdater.get( this ) == State.CHECKED_OUT;
    }

    public long getLastAccess() {
        return lastAccess;
    }

    public void setLastAccess(long lastAccess) {
        this.lastAccess = lastAccess;
    }

    public Thread getHoldingThread() {
        return holdingThread;
    }

    public void setHoldingThread(Thread holdingThread) {
        this.holdingThread = holdingThread;
    }

    public void setDirtyAttribute(DirtyAttribute attribute) {
        dirtyAttributes.add( attribute );
    }

    public boolean isEnlisted() {
        return enlisted;
    }

    public void setEnlisted() {
        enlisted = true;
    }

    public void resetEnlisted() {
        enlisted = false;
    }

    // --- //

    public enum State {
        NEW, CHECKED_IN, CHECKED_OUT, VALIDATION, FLUSH, DESTROYED
    }

    public enum DirtyAttribute {
        AUTOCOMMIT, TRANSACTION_ISOLATION, NETWORK_TIMEOUT, SCHEMA, CATALOG
    }
}
