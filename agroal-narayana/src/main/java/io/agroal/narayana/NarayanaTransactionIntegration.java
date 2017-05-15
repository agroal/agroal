// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.narayana;

import io.agroal.api.transaction.TransactionAware;
import io.agroal.api.transaction.TransactionIntegration;

import javax.transaction.Synchronization;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

import static javax.transaction.Status.STATUS_ACTIVE;
import static javax.transaction.Status.STATUS_MARKED_ROLLBACK;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class NarayanaTransactionIntegration implements TransactionIntegration {

    private final TransactionManager transactionManager;

    private final TransactionSynchronizationRegistry transactionSynchronizationRegistry;

    // In order to construct a UID that is globally unique, simply pair a UID with an InetAddress.
    private final UUID key = UUID.randomUUID();

    public NarayanaTransactionIntegration(TransactionManager transactionManager, TransactionSynchronizationRegistry transactionSynchronizationRegistry) {
        this.transactionManager = transactionManager;
        this.transactionSynchronizationRegistry = transactionSynchronizationRegistry;
    }

    @Override
    public Connection getConnection() throws SQLException {
        if ( transactionRunning() ) {
            return (Connection) transactionSynchronizationRegistry.getResource( key );
        }
        return null;
    }

    @Override
    public void associate(Connection connection) throws SQLException {
        try {
            if ( transactionRunning() ) {
                boolean newEnlistment = transactionSynchronizationRegistry.getResource( key ) == null;
                transactionSynchronizationRegistry.registerInterposedSynchronization( new InterposedSynchronization( connection ) );

                if ( newEnlistment ) {
                    transactionSynchronizationRegistry.putResource( key, connection );
                    transactionManager.getTransaction().enlistResource( new LocalXAResource( (TransactionAware) connection ) );
                }
                else {
                    ( (TransactionAware) connection ).transactionStart();
                }
            } else {
                ( (TransactionAware) connection ).transactionCheckCallback( this::transactionRunning );
            }
        } catch ( Exception e ) {
            throw new SQLException( "Exception in association of connection to existing transaction", e );
        }
    }

    @Override
    public boolean disassociate(Connection connection) throws SQLException {
        if ( transactionRunning() ) {
            transactionSynchronizationRegistry.putResource( key, null );
        }
        return true;
    }

    private boolean transactionRunning() throws SQLException {
        try {
            Transaction transaction = transactionManager.getTransaction();
            return transaction != null && ( transaction.getStatus() == STATUS_ACTIVE || transaction.getStatus() == STATUS_MARKED_ROLLBACK );
        } catch ( Exception e ) {
            throw new SQLException( "Exception in retrieving existing transaction", e );
        }
    }

    // --- //

    private static class InterposedSynchronization implements Synchronization {

        private final Connection connection;

        private InterposedSynchronization(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void beforeCompletion() {
        }

        @Override
        public void afterCompletion(int status) {
            try { // Return connection to the pool
                connection.close();
            } catch ( SQLException ignore ) {
            }
        }
    }

}
