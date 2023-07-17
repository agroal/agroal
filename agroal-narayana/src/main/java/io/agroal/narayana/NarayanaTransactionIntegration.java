// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.narayana;

import io.agroal.api.transaction.TransactionAware;
import io.agroal.api.transaction.TransactionIntegration;
import jakarta.transaction.Synchronization;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.TransactionSynchronizationRegistry;
import org.jboss.tm.TxUtils;
import org.jboss.tm.XAResourceRecovery;
import org.jboss.tm.XAResourceRecoveryRegistry;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.agroal.narayana.NarayanaTransactionIntegration.TransactionPhase.TRANSACTION_ACTIVE;
import static io.agroal.narayana.NarayanaTransactionIntegration.TransactionPhase.TRANSACTION_COMPLETING;
import static io.agroal.narayana.NarayanaTransactionIntegration.TransactionPhase.TRANSACTION_DONE;
import static io.agroal.narayana.NarayanaTransactionIntegration.TransactionPhase.TRANSACTION_NONE;
import static jakarta.transaction.Status.*;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class NarayanaTransactionIntegration implements TransactionIntegration {

    // Use this cache as method references are not stable (they are used as bridge between RecoveryConnectionFactory and XAResourceRecovery)
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
        if ( getTransactionPhase() == TRANSACTION_ACTIVE ) {
            return (TransactionAware) transactionSynchronizationRegistry.getResource( key );
        }
        return null;
    }

    // --- //

    public enum TransactionPhase {
        // these states are a coarser version of the transaction states
        TRANSACTION_NONE, TRANSACTION_ACTIVE, TRANSACTION_COMPLETING, TRANSACTION_DONE
    }

    @Override
    public void associate(TransactionAware transactionAware, XAResource xaResource) throws SQLException {
        try {
            TransactionPhase phase = getTransactionPhase();
            if ( phase == TRANSACTION_ACTIVE ) {
                if ( transactionSynchronizationRegistry.getResource( key ) == null ) {
                    transactionSynchronizationRegistry.registerInterposedSynchronization( new InterposedSynchronization( transactionAware ) );
                    transactionSynchronizationRegistry.putResource( key, transactionAware );

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
            }
            // AG-209 - if a transaction is completing, ensure that the transaction state does not change
            transactionAware.transactionCheckCallback( phase == TRANSACTION_COMPLETING ? getChangeStateCallback() : this::transactionRunning );
        } catch ( Exception e ) {
            throw new SQLException( "Exception in association of connection to existing transaction", e );
        }
    }

    @Override
    public boolean disassociate(TransactionAware connection) throws SQLException {
        if ( getTransactionPhase() == TRANSACTION_ACTIVE ) {
            transactionSynchronizationRegistry.putResource( key, null );
        }
        return true;
    }

    private boolean transactionRunning() throws SQLException {
        TransactionPhase phase = getTransactionPhase();
        return phase == TRANSACTION_ACTIVE || phase == TRANSACTION_COMPLETING;
    }

    private TransactionPhase getTransactionPhase() throws SQLException {
        try {
            Transaction transaction = transactionManager.getTransaction();
            if ( transaction == null ) {
                // AG-183 - Report running transaction when reaper thread attempts rollback
                return TxUtils.isTransactionManagerTimeoutThread() ? TRANSACTION_COMPLETING : TRANSACTION_NONE;
            }
            switch ( transaction.getStatus() ) {
                default:
                case STATUS_UNKNOWN:
                case STATUS_NO_TRANSACTION:
                    return TRANSACTION_NONE;
                case STATUS_ACTIVE:
                case STATUS_MARKED_ROLLBACK:
                    return TRANSACTION_ACTIVE;
                case STATUS_PREPARING:
                case STATUS_PREPARED:
                case STATUS_COMMITTING:
                case STATUS_ROLLING_BACK:
                    return TRANSACTION_COMPLETING;
                case STATUS_COMMITTED:
                case STATUS_ROLLEDBACK:
                    return TRANSACTION_DONE;
            }
        } catch ( Exception e ) {
            throw new SQLException( "Exception in retrieving existing transaction", e );
        }
    }

    private TransactionAware.SQLCallable<Boolean> getChangeStateCallback() throws SQLException {
        try {
            Transaction transaction = transactionManager.getTransaction();
            if ( transaction == null ) {
                throw new SQLException( "Expecting existing transaction" );
            }
            int status = transaction.getStatus();
            return () -> {
                try {
                    return transactionManager.getTransaction().getStatus() != status;
                } catch ( Exception e ) {
                    throw new SQLException( e );
                }
            };
        } catch ( Exception e ) {
            throw new SQLException( "Exception in retrieving existing transaction", e );
        }
    }

    // -- //

    @Override
    public void addResourceRecoveryFactory(ResourceRecoveryFactory factory) {
        if ( recoveryRegistry != null ) {
            recoveryRegistry.addXAResourceRecovery( resourceRecoveryCache.computeIfAbsent( factory, f -> new AgroalXAResourceRecovery( f, jndiName ) ) );
        }
    }

    @Override
    public void removeResourceRecoveryFactory(ResourceRecoveryFactory factory) {
        if ( recoveryRegistry != null ) {
            recoveryRegistry.removeXAResourceRecovery( resourceRecoveryCache.remove( factory ) );
        }
    }

    // --- //

    // This auxiliary class is a contraption due to the fact that XAResource is not closable.
    // It creates RecoveryXAResource wrappers that keeps track of lifecycle and closes the associated connection.
    private static class AgroalXAResourceRecovery implements XAResourceRecovery {

        private static final XAResource[] EMPTY_RESOURCES = new XAResource[0];

        private final ResourceRecoveryFactory connectionFactory;
        private final String name;

        @SuppressWarnings( "WeakerAccess" )
        AgroalXAResourceRecovery(ResourceRecoveryFactory factory, String jndiName) {
            connectionFactory = factory;
            name = jndiName;
        }

        @Override
        @SuppressWarnings( "resource" )
        public XAResource[] getXAResources() {
            XAConnection xaConnection = connectionFactory.getRecoveryConnection();
            try {
                return xaConnection == null ? EMPTY_RESOURCES : new XAResource[]{new RecoveryXAResource( xaConnection, name )};
            } catch ( SQLException e ) {
                return new XAResource[]{new ErrorConditionXAResource( xaConnection, e, name )};
            }
        }
    }

    private static class InterposedSynchronization implements Synchronization {

        private final TransactionAware transactionAware;

        @SuppressWarnings( "WeakerAccess" )
        InterposedSynchronization(TransactionAware transactionAware) {
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
