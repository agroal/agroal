// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.transaction;

import java.sql.SQLException;

/**
 * Interface to be implemented by a resource (a connection) that the transaction integration layer will manipulate.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public interface TransactionAware {

    /**
     * The resource it's now enlisted with a transaction.
     */
    void transactionStart() throws SQLException;

    /**
     * The resource must commit.
     */
    void transactionCommit() throws SQLException;

    /**
     * The resource must rollback.
     */
    void transactionRollback() throws SQLException;

    /**
     * The transaction ended and the resource is no longer enlisted.
     */
    void transactionEnd() throws SQLException;

    /**
     * Set a callback trap to prevent lazy / deferred enlistment. Agroal supports neither of those features.
     * This callback is set when the resource is obtained outside the scope of a running transaction and allows the resource to check if it's used within a transaction later on.
     */
    void transactionCheckCallback(SQLCallable<Boolean> transactionCheck);

    /**
     * Gets access to the raw {@link java.sql.Connection} held by the resource.
     */
    Object getConnection();

    /**
     * The resource is no longer valid and should not be returned to the pool.
     */
    void setFlushOnly();

    // --- //

    /**
     * A callable that can throw {@link SQLException}
     */
    @FunctionalInterface
    interface SQLCallable<T> {

        /**
         * Execute an action that may result in an {@link SQLException}
         */
        T call() throws SQLException;
    }
}
