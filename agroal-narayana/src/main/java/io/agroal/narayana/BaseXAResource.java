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
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
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
        try {
            xaResource.commit( xid, onePhase );
        } catch ( XAException xe ) {
            connection.setFlushOnly();
            throw xe;
        }
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        try {
            xaResource.end( xid, flags );
            connection.transactionEnd();
        } catch ( Exception t ) {
            connection.setFlushOnly();
            throw new XAException( "Error trying to end xa transaction: " + t.getMessage() );
        }
    }

    @Override
    public void forget(Xid xid) throws XAException {
        try {
            xaResource.forget( xid );
        } catch ( XAException xe ) {
            connection.setFlushOnly();
            throw xe;
        }
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        try {
            return xaResource.getTransactionTimeout();
        } catch ( XAException xe ) {
            connection.setFlushOnly();
            throw xe;
        }
    }

    @Override
    public boolean isSameRM(XAResource xaRes) throws XAException {
        try {
            return xaResource.isSameRM( xaRes );
        } catch ( XAException xe ) {
            connection.setFlushOnly();
            throw xe;
        }
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        try {
            return xaResource.prepare( xid );
        } catch ( XAException xe ) {
            connection.setFlushOnly();
            throw xe;
        }
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        try {
            return xaResource.recover( flag );
        } catch ( XAException xe ) {
            connection.setFlushOnly();
            throw xe;
        }
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        try {
            xaResource.rollback( xid );
        } catch ( XAException xe ) {
            connection.setFlushOnly();
            throw xe;
        }
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
        try {
            return xaResource.setTransactionTimeout( seconds );
        } catch ( XAException xe ) {
            connection.setFlushOnly();
            throw xe;
        }
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
        try {
            connection.transactionStart();
            xaResource.start( xid, flags );
        } catch ( Exception e ) {
            connection.setFlushOnly();
            XAException xe = new XAException( "Error trying to start xa transaction: " + e.getMessage() );
            xe.initCause( e );
            throw xe;
        }
    }
}
