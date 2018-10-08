// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.transaction;

import javax.transaction.xa.XAResource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface TransactionIntegration {

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

    TransactionAware getTransactionAware() throws SQLException;

    void associate(TransactionAware transactionAware, XAResource xaResource) throws SQLException;

    boolean disassociate(TransactionAware transactionAware) throws SQLException;

    // --- //

    void addResourceRecoveryFactory(ResourceRecoveryFactory factory);

    void removeResourceRecoveryFactory(ResourceRecoveryFactory factory);

    interface ResourceRecoveryFactory {

        XAResource[] recoveryResources();
    }
}
