// Copyright (C) 2026 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper.closed;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * Sentinel implementation of {@link XAResource} that throws {@link XAException} on all operations.
 *
 * @author <a href="gegastaldi@gmail.com">George Gastaldi</a>
 */
public final class ClosedXAResource implements XAResource {

    public static final ClosedXAResource INSTANCE = new ClosedXAResource();

    // --- //

    private ClosedXAResource() {
    }

    private static XAException closed() {
        return new XAException( "XAConnection for the XAResource is closed" );
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        throw closed();
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        throw closed();
    }

    @Override
    public void forget(Xid xid) throws XAException {
        throw closed();
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        throw closed();
    }

    @Override
    public boolean isSameRM(XAResource xares) throws XAException {
        throw closed();
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        throw closed();
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        throw closed();
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        throw closed();
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
        throw closed();
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
        throw closed();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
