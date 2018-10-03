// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import static java.lang.System.identityHashCode;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface MockXAResource extends XAResource {

    @Override
    default void commit(Xid xid, boolean b) throws XAException {
    }

    @Override
    default void end(Xid xid, int i) throws XAException {
    }

    @Override
    default void forget(Xid xid) throws XAException {
    }

    @Override
    default int getTransactionTimeout() throws XAException {
        return 0;
    }

    @Override
    default boolean isSameRM(XAResource xaResource) throws XAException {
        return false;
    }

    @Override
    default int prepare(Xid xid) throws XAException {
        return 0;
    }

    @Override
    default Xid[] recover(int i) throws XAException {
        return null;
    }

    @Override
    default void rollback(Xid xid) throws XAException {
    }

    @Override
    default boolean setTransactionTimeout(int i) throws XAException {
        return false;
    }

    @Override
    default void start(Xid xid, int i) throws XAException {
    }

    // --- //

    class Empty implements MockXAResource {

        @Override
        public String toString() {
            return "MockXAResource@" + identityHashCode( this );
        }
    }
}
