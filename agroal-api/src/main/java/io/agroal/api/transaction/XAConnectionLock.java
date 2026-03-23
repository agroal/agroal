// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.transaction;

import java.sql.SQLException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Serializes JDBC operations with XA branch termination on a single connection.
 *
 * When Narayana's TransactionReaper forces an abort, it calls XAResource.end(TMFAIL)
 * and XAResource.rollback() from the reaper thread while the application thread may
 * still be executing SQL. After end(TMFAIL), the connection silently exits XA mode
 * and subsequent SQL operations execute as unmanaged local operations outside any
 * transaction, breaking atomicity.
 *
 * A ReadWriteLock where JDBC execute operations hold the read lock and XA branch
 * termination holds the write lock. The write lock blocks until in-flight JDBC
 * completes, then atomically terminates the XA branch and marks the connection
 * as poisoned.
 *
 * @author Red Hat
 */
public final class XAConnectionLock {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private volatile boolean poisoned;
    private String poisonReason;

    /**
     * Acquire the read lock for a JDBC operation.
     * Blocks if XA termination (write lock) is in progress.
     * Throws if the connection has been poisoned.
     *
     * If this method throws, the lock is NOT held and releaseForJdbc() MUST NOT be called.
     * If this method returns normally, the caller MUST call releaseForJdbc() in a finally block.
     */
    public void acquireForJdbc() throws SQLException {
        lock.readLock().lock();
        if ( poisoned ) {
            lock.readLock().unlock();
            throw new SQLException( "Connection is not usable: " + poisonReason );
        }
    }

    public void releaseForJdbc() {
        lock.readLock().unlock();
    }

    /**
     * Acquire the write lock for XA branch termination.
     * Blocks until all in-flight JDBC operations on this connection complete.
     */
    public void acquireForXaEnd() {
        lock.writeLock().lock();
    }

    public void releaseForXaEnd() {
        lock.writeLock().unlock();
    }

    /**
     * Mark the connection as poisoned. Must be called under write lock.
     */
    public void markPoisoned(String reason) {
        this.poisonReason = reason;
        this.poisoned = true;
    }

    public boolean isPoisoned() {
        return poisoned;
    }

    public String getPoisonReason() {
        return poisonReason;
    }

    /**
     * Reset the lock state for connection reuse from the pool.
     */
    public void reset() {
        if ( !poisoned ) {
            return;
        }
        lock.writeLock().lock();
        try {
            poisoned = false;
            poisonReason = null;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
