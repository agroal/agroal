package io.agroal.narayana;

import org.jboss.tm.XAResourceWrapper;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.SQLException;

public class ErrorConditionXAResource implements AutoCloseable, XAResourceWrapper {

    private static final String PRODUCT_NAME = ErrorConditionXAResource.class.getPackage().getImplementationTitle();
    private static final String PRODUCT_VERSION = ErrorConditionXAResource.class.getPackage().getImplementationVersion();

    private final XAConnection xaConnection;
    private final SQLException error;
    private final String jndiName;

    public ErrorConditionXAResource(XAConnection xaConnection, SQLException error, String jndiName) {
        this.xaConnection = xaConnection;
        this.error = error;
        this.jndiName = jndiName;
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        if ( flag == TMENDRSCAN ) {
            close();
        }
        throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, error );
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, error );
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, error );
    }

    @Override
    public void forget(Xid xid) throws XAException {
        throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, error );
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, error );
    }

    @Override
    public boolean isSameRM(XAResource xares) throws XAException {
        throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, error );
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, error );
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, error );
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
        throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, error );
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
        throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, error );
    }

    // --- //

    @Override
    public void close() throws XAException {
        try {
            xaConnection.close();
        } catch ( SQLException e ) {
            throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, e );
        }
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
