// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.narayana;

import io.agroal.api.transaction.TransactionAware;
import io.agroal.api.transaction.XAConnectionLock;
import org.jboss.tm.XAResourceWrapper;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class BaseXAResource implements XAResourceWrapper {

    private static final String PRODUCT_NAME = BaseXAResource.class.getPackage().getImplementationTitle();
    private static final String PRODUCT_VERSION = BaseXAResource.class.getPackage().getImplementationVersion();

    private final TransactionAware transactionAware;
    private final XAResource xaResource;
    private final String jndiName;
    private final XAConnectionLock xaConnectionLock;

    public BaseXAResource(TransactionAware transactionAware, XAResource xaResource, String jndiName, XAConnectionLock xaConnectionLock) {
        this.transactionAware = transactionAware;
        this.xaResource = xaResource;
        this.jndiName = jndiName;
        this.xaConnectionLock = xaConnectionLock;
    }

    @Override
    public XAResource getResource() {
        return xaResource;
    }

    @Override
    public String getProductName() {
        return PRODUCT_NAME;
    }

    @Override
    public String getProductVersion() {
        return PRODUCT_VERSION;
    }

    @Override
    public String getJndiName() {
        return jndiName;
    }

    // --- //

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        try {
            transactionAware.transactionBeforeCompletion( true );
            xaResource.commit( xid, onePhase );
        } catch ( XAException xe ) {
            transactionAware.setFlushOnly();
            throw xe;
        }
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        if ( ( flags & TMFAIL ) != 0 ) {
            endWithXaLock( xid, flags );
        } else {
            try {
                xaResource.end( xid, flags );
            } catch ( XAException xe ) {
                if ( !XAExceptionUtils.isUnilateralRollbackOnAbort( xe.errorCode, flags ) ) {
                    transactionAware.setFlushOnly();
                }
                throw xe;
            } catch ( Exception e ) {
                transactionAware.setFlushOnly();
                throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, "Error trying to end xa transaction: ", e );
            }
        }
    }

    /**
     * end(TMFAIL) under the XAConnectionLock write lock.
     * Blocks until all in-flight JDBC operations (holding the read lock) complete,
     * then executes the driver's end(TMFAIL) and poisons the connection.
     */
    private void endWithXaLock(Xid xid, int flags) throws XAException {
        XAConnectionLock xaLock = xaConnectionLock;
        if ( xaLock == null ) {
            try {
                xaResource.end( xid, flags );
            } catch ( XAException xe ) {
                if ( !XAExceptionUtils.isUnilateralRollbackOnAbort( xe.errorCode, flags ) ) {
                    transactionAware.setFlushOnly();
                }
                throw xe;
            }
            return;
        }

        xaLock.acquireForXaEnd();
        try {
            xaResource.end( xid, flags );
        } catch ( XAException xe ) {
            if ( !XAExceptionUtils.isUnilateralRollbackOnAbort( xe.errorCode, flags ) ) {
                transactionAware.setFlushOnly();
            }
            throw xe;
        } catch ( Exception e ) {
            transactionAware.setFlushOnly();
            throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, "Error trying to end xa transaction: ", e );
        } finally {
            xaLock.markPoisoned( "XA branch ended with TMFAIL" );
            xaLock.releaseForXaEnd();
        }
    }

    @Override
    public void forget(Xid xid) throws XAException {
        try {
            xaResource.forget( xid );
        } catch ( XAException xe ) {
            transactionAware.setFlushOnly();
            throw xe;
        }
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        try {
            return xaResource.getTransactionTimeout();
        } catch ( XAException xe ) {
            transactionAware.setFlushOnly();
            throw xe;
        }
    }

    @Override
    public boolean isSameRM(XAResource xaRes) throws XAException {
        try {
            return xaResource.isSameRM( xaRes );
        } catch ( XAException xe ) {
            transactionAware.setFlushOnly();
            throw xe;
        }
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        try {
            return xaResource.prepare( xid );
        } catch ( XAException xe ) {
            transactionAware.setFlushOnly();
            throw xe;
        }
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        try {
            return xaResource.recover( flag );
        } catch ( XAException xe ) {
            transactionAware.setFlushOnly();
            throw xe;
        }
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        XAConnectionLock xaLock = xaConnectionLock;
        if ( xaLock != null ) {
            xaLock.acquireForXaEnd();
        }
        try {
            transactionAware.transactionBeforeCompletion( false );
            xaResource.rollback( xid );
        } catch ( XAException xe ) {
            transactionAware.setFlushOnly();
            throw xe;
        } finally {
            if ( xaLock != null ) {
                if ( !xaLock.isPoisoned() ) {
                    xaLock.markPoisoned( "XA branch rolled back" );
                }
                xaLock.releaseForXaEnd();
            }
        }
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
        try {
            return xaResource.setTransactionTimeout( seconds );
        } catch ( XAException xe ) {
            transactionAware.setFlushOnly();
            throw xe;
        }
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
        try {
            transactionAware.transactionStart();
            xaResource.start( xid, flags );

            XAConnectionLock xaLock = xaConnectionLock;
            if ( xaLock != null ) {
                xaLock.reset();
            }
        } catch ( XAException xe ) {
            transactionAware.setFlushOnly();
            throw xe;
        } catch ( Exception e ) {
            transactionAware.setFlushOnly();
            throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, "Error trying to start xa transaction: ", e );
        }
    }

}
