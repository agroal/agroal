package io.agroal.narayana;

import org.jboss.tm.XAResourceWrapper;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.SQLException;

public class ErrorConditionXAResource implements XAResourceWrapper {

    private static final String PRODUCT_NAME = ErrorConditionXAResource.class.getPackage().getImplementationTitle();
    private static final String PRODUCT_VERSION = ErrorConditionXAResource.class.getPackage().getImplementationVersion();

    private final SQLException error;
    private final String jndiName;

    public ErrorConditionXAResource(SQLException e, String jndiName) {
        this.error = e;
        this.jndiName = jndiName;
    }

    private XAException createXAException() {
        XAException xaException = new XAException( XAException.XAER_RMFAIL );
        xaException.initCause( error );
        return xaException;
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        throw createXAException();
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        throw createXAException();
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        throw createXAException();
    }

    @Override
    public void forget(Xid xid) throws XAException {
        throw createXAException();
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        throw createXAException();
    }

    @Override
    public boolean isSameRM(XAResource xares) throws XAException {
        throw createXAException();
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        throw createXAException();
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        throw createXAException();
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
        throw createXAException();
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
        throw createXAException();
    }

    // --- //

    @Override
    public XAResource getResource() {
        return null;
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
}
