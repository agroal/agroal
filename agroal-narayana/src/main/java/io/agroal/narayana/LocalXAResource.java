// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.narayana;

import io.agroal.api.transaction.TransactionAware;
import org.jboss.tm.ConnectableResource;
import org.jboss.tm.LastResource;
import org.jboss.tm.XAResourceWrapper;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class LocalXAResource implements XAResource, ConnectableResource, LastResource, XAResourceWrapper {

    private final TransactionAware connection;

    private Xid currentXid;

    private String productName;

    private String productVersion;

    private String jndiName;

    public LocalXAResource(TransactionAware connection) {
        this.connection = connection;
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
        if ( currentXid != null ) {
            if ( flags != TMJOIN && flags != TMRESUME ) {
                throw new XAException( XAException.XAER_DUPID );
            }
        } else {
            if ( flags != TMNOFLAGS ) {
                throw new XAException( "Starting resource with wrong flags" );
            }
            try {
                connection.transactionStart();
            } catch ( Throwable t ) {
                throw new XAException( "Error trying to start local transaction: " + t.getMessage() );
            }
            currentXid = xid;
        }
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        if ( xid == null || !xid.equals( currentXid ) ) {
            throw new XAException( "Invalid xid to transactionCommit" );
        }
        currentXid = null;

        try {
            connection.transactionCommit();
            connection.transactionEnd();
        } catch ( Throwable t ) {
            throw new XAException( "Error trying to transactionCommit local transaction: " + t.getMessage() );
        }
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        if ( xid == null || !xid.equals( currentXid ) ) {
            throw new XAException( "Invalid xid to transactionCommit" );
        }
        currentXid = null;

        try {
            connection.transactionRollback();
            connection.transactionEnd();
        } catch ( Throwable t ) {
            throw new XAException( "Error trying to transactionRollback local transaction: " + t.getMessage() );
        }
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
    }

    @Override
    public void forget(Xid xid) throws XAException {
        throw new XAException( "Forget not supported in local XA resource" );
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return 0;
    }

    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException {
        return this == xaResource;
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        return XA_OK;
    }

    @Override
    public Xid[] recover(int flags) throws XAException {
        throw new XAException( "No recover in local XA resource" );
    }

    @Override
    public boolean setTransactionTimeout(int timeout) throws XAException {
        return false;
    }

    // --- XA Resource Wrapper //

    @Override
    public XAResource getResource() {
        return this;
    }

    @Override
    public String getProductName() {
        return productName;
    }

    @Override
    public String getProductVersion() {
        return productVersion;
    }

    @Override
    public String getJndiName() {
        return jndiName;
    }

    @Override
    public Object getConnection() throws Throwable {
        return connection;
    }
}
