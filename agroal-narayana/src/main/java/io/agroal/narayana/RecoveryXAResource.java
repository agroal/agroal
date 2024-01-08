package io.agroal.narayana;

import io.agroal.api.transaction.TransactionIntegration.ResourceRecoveryFactory;
import org.jboss.tm.XAResourceWrapper;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.SQLException;

public class RecoveryXAResource implements AutoCloseable, XAResourceWrapper {

    private static final String PRODUCT_NAME = RecoveryXAResource.class.getPackage().getImplementationTitle();
    private static final String PRODUCT_VERSION = RecoveryXAResource.class.getPackage().getImplementationVersion();

    private final ResourceRecoveryFactory connectionFactory;
    private final String jndiName;
    private XAConnection xaConnection;
    private XAResource wrappedResource;

    public RecoveryXAResource(ResourceRecoveryFactory factory, String name) throws SQLException {
        connectionFactory = factory;
        jndiName = name;
        connect();
    }

    private void connect() throws SQLException {
        if ( wrappedResource == null ) {
            xaConnection = connectionFactory.getRecoveryConnection();
            wrappedResource = xaConnection.getXAResource();
        }
    }

    private XAResource getConnectedResource() throws XAException {
        try {
            connect();
        } catch ( SQLException e ) {
            throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, e );
        }
        return wrappedResource;
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        Xid[] value = getConnectedResource().recover( flag );
        if ( flag == TMENDRSCAN && ( value == null || value.length == 0 ) ) {
            close();
        }
        return value;
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        getConnectedResource().commit( xid, onePhase );
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        getConnectedResource().end( xid, flags );
    }

    @Override
    public void forget(Xid xid) throws XAException {
        getConnectedResource().forget( xid );
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return getConnectedResource().getTransactionTimeout();
    }

    @Override
    public boolean isSameRM(XAResource xares) throws XAException {
        return getConnectedResource().isSameRM( xares );
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        return getConnectedResource().prepare( xid );
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        getConnectedResource().rollback( xid );
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
        return getConnectedResource().setTransactionTimeout( seconds );
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
        getConnectedResource().start( xid, flags );
    }

    // --- //

    @Override
    public void close() throws XAException {
        try {
            xaConnection.close();
        } catch ( SQLException e ) {
            throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, e );
        } finally {
            xaConnection = null;
            wrappedResource = null;
        }
    }

    // --- //

    @Override
    public XAResource getResource() {
        try {
            return getConnectedResource();
        } catch (XAException e) {
            throw new IllegalStateException(e);
        }
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
