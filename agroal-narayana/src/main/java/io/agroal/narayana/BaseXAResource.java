// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.narayana;

import io.agroal.api.transaction.TransactionAware;
import org.jboss.tm.XAResourceWrapper;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class BaseXAResource implements XAResourceWrapper {

    private static final String PRODUCT_NAME = BaseXAResource.class.getPackage().getImplementationTitle();
    private static final String PRODUCT_VERSION = BaseXAResource.class.getPackage().getImplementationVersion();

    private final TransactionAware connection;
    private final XAResource xaResource;
    private final String jndiName;

    public BaseXAResource(TransactionAware connection, XAResource xaResource, String jndiName) {
        this.connection = connection;
        this.xaResource = xaResource;
        this.jndiName = jndiName;
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
        xaResource.commit( xid, onePhase );
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        xaResource.end( xid, flags );

        try {
            connection.transactionEnd();
        } catch ( Exception t ) {
            throw new XAException( "Error trying to end xa transaction: " + t.getMessage() );
        }
    }

    @Override
    public void forget(Xid xid) throws XAException {
        xaResource.forget( xid );
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return xaResource.getTransactionTimeout();
    }

    @Override
    public boolean isSameRM(XAResource xaRes) throws XAException {
        return xaResource.isSameRM( xaRes );
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        return xaResource.prepare( xid );
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        return xaResource.recover( flag );
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        xaResource.rollback( xid );
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
        return xaResource.setTransactionTimeout( seconds );
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
        try {
            connection.transactionStart();
        } catch ( Exception t ) {
            throw new XAException( "Error trying to start xa transaction: " + t.getMessage() );
        }
        xaResource.start( xid, flags );
    }
}
