// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.narayana;

import io.agroal.api.transaction.TransactionAware;
import io.agroal.api.transaction.TransactionIntegration;
import org.jboss.tm.XAResourceRecovery;
import org.jboss.tm.XAResourceRecoveryRegistry;

import javax.transaction.Synchronization;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;
import javax.transaction.xa.XAResource;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static javax.transaction.Status.STATUS_ACTIVE;
import static javax.transaction.Status.STATUS_MARKED_ROLLBACK;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class NarayanaTransactionIntegration implements TransactionIntegration {

    // Use this cache as method references are not stable (they are used as bridge between ResourceRecoveryFactory and XAResourceRecovery)
    private static final ConcurrentMap<ResourceRecoveryFactory, XAResourceRecovery> resourceRecoveryCache = new ConcurrentHashMap<>();

    private final TransactionManager transactionManager;

    private final TransactionSynchronizationRegistry transactionSynchronizationRegistry;

    private final String jndiName;

    private final boolean connectable;

    private final XAResourceRecoveryRegistry recoveryRegistry;

    // In order to construct a UID that is globally unique, simply pair a UID with an InetAddress.
    private final UUID key = UUID.randomUUID();

    public NarayanaTransactionIntegration(TransactionManager transactionManager, TransactionSynchronizationRegistry transactionSynchronizationRegistry) {
        this( transactionManager, transactionSynchronizationRegistry, null, false );
    }

    public NarayanaTransactionIntegration(TransactionManager transactionManager, TransactionSynchronizationRegistry transactionSynchronizationRegistry, String jndiName) {
        this( transactionManager, transactionSynchronizationRegistry, jndiName, false );
    }

    public NarayanaTransactionIntegration(TransactionManager transactionManager, TransactionSynchronizationRegistry transactionSynchronizationRegistry, String jndiName, boolean connectable) {
        this( transactionManager, transactionSynchronizationRegistry, jndiName, connectable, null );
    }

    public NarayanaTransactionIntegration(TransactionManager transactionManager, TransactionSynchronizationRegistry transactionSynchronizationRegistry, String jndiName, boolean connectable, XAResourceRecoveryRegistry recoveryRegistry) {
        this.transactionManager = transactionManager;
        this.transactionSynchronizationRegistry = transactionSynchronizationRegistry;
        this.jndiName = jndiName;
        this.connectable = connectable;
        this.recoveryRegistry = recoveryRegistry;
    }

    @Override
    public TransactionAware getTransactionAware() throws SQLException {
        if ( transactionRunning() ) {
            return (TransactionAware) transactionSynchronizationRegistry.getResource( key );
        }
        return null;
    }

    @Override
    public void associate(TransactionAware transactionAware, XAResource xaResource) throws SQLException {
        try {
            if ( transactionRunning() ) {
                if ( transactionSynchronizationRegistry.getResource( key ) == null ) {
                    transactionSynchronizationRegistry.putResource( key, transactionAware );
                    transactionSynchronizationRegistry.registerInterposedSynchronization( new InterposedSynchronization( transactionAware ) );

                    XAResource xaResourceToEnlist;
                    if ( xaResource != null ) {
                        xaResourceToEnlist = new BaseXAResource( transactionAware, xaResource, jndiName );
                    } else if ( connectable ) {
                        xaResourceToEnlist = new ConnectableLocalXAResource( transactionAware, jndiName );
                    } else {
                        xaResourceToEnlist = new LocalXAResource( transactionAware, jndiName );
                    }
                    transactionManager.getTransaction().enlistResource( xaResourceToEnlist );
                } else {
                    transactionAware.transactionStart();
                }
            } else {
                transactionAware.transactionCheckCallback( this::transactionRunning );
            }
        } catch ( Exception e ) {
            throw new SQLException( "Exception in association of connection to existing transaction", e );
        }
    }

    @Override
    public boolean disassociate(TransactionAware connection) throws SQLException {
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

    // -- //

    @Override
    public void addResourceRecoveryFactory(ResourceRecoveryFactory factory) {
        if ( recoveryRegistry != null ) {
            recoveryRegistry.addXAResourceRecovery( resourceRecoveryCache.computeIfAbsent( factory, f -> f::recoveryResources ) );
        }
    }

    @Override
    public void removeResourceRecoveryFactory(ResourceRecoveryFactory factory) {
        if ( recoveryRegistry != null ) {
            recoveryRegistry.removeXAResourceRecovery( resourceRecoveryCache.remove( factory ) );
        }
    }

    // --- //

    private static class InterposedSynchronization implements Synchronization {

        private final TransactionAware transactionAware;

        private InterposedSynchronization(TransactionAware transactionAware) {
            this.transactionAware = transactionAware;
        }

        @Override
        public void beforeCompletion() {
            // nothing to do
        }

        @Override
        public void afterCompletion(int status) {
            // Return connection to the pool
            try {
                transactionAware.transactionEnd();
            } catch ( SQLException ignore ) {
                // ignore
            }
        }
    }
}
