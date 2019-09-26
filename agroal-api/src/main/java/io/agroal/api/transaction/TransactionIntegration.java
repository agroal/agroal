// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.transaction;

import javax.transaction.xa.XAResource;
import java.sql.SQLException;

/**
 * Agroal provides an integration point for transaction systems to modify the behaviour of the pool.
 * The transaction layer can control the lifecycle of connections and define what connections are acquired and when these return to the pool.
 * It is responsible for commit or rollback of the database transaction.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface TransactionIntegration {

    /**
     * The default implementation of the transaction layer, that performs no actions.
     */
    static TransactionIntegration none() {
        return new TransactionIntegration() {

            @Override
            public TransactionAware getTransactionAware() {
                return null;
            }

            @Override
            public void associate(TransactionAware connection, XAResource xaResource) {
                // nothing to do
            }

            @Override
            public boolean disassociate(TransactionAware connection) {
                return true;
            }

            @Override
            public void addResourceRecoveryFactory(ResourceRecoveryFactory factory) {
                // nothing to do
            }

            @Override
            public void removeResourceRecoveryFactory(ResourceRecoveryFactory factory) {
                // nothing to do
            }
        };
    }

    // --- //

    /**
     * Agroal inquires the transaction layer for a Tx aware resource (a connection) that can be acquired.
     * Usually, if there is a resource already associated with the calling thread it is returned.
     */
    TransactionAware getTransactionAware() throws SQLException;

    /**
     * Agroal notifies the transaction layer a Tx aware resource (a connection) and it's corresponding XA resource were acquired.
     * Usually, the resource is associated with the calling thread.
     */
    void associate(TransactionAware transactionAware, XAResource xaResource) throws SQLException;

    /**
     * Agroal notifies the transaction layer that a Tx aware resource (a connection) is about to be returned to the pool.
     * Usually, the resource is disassociated from the calling thread and returned to the pool.
     *
     * @return true if the Tx aware should return to the pool, false if it should not.
     */
    boolean disassociate(TransactionAware transactionAware) throws SQLException;

    // --- //

    /**
     * Agroal calls this method on init to register itself as a XA module capable of recovery.
     */
    void addResourceRecoveryFactory(ResourceRecoveryFactory factory);

    /**
     * Agroal calls this method on shutdown to de-register itself as a XA module capable of recovery.
     */
    void removeResourceRecoveryFactory(ResourceRecoveryFactory factory);

    /**
     * This interface is implemented by the connection factory so that it can provide recovery resources to the transaction layer.
     */
    interface ResourceRecoveryFactory {

        /**
         * The transaction layer can call this method to obtain resources (one connection) used for recovery of incomplete transactions.
         */
        XAResource[] recoveryResources();
    }
}
