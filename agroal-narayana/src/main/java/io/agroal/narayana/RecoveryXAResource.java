package io.agroal.narayana;

import org.jboss.tm.XAResourceWrapper;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.SQLException;

public class RecoveryXAResource implements AutoCloseable, XAResourceWrapper {

    private static final String PRODUCT_NAME = RecoveryXAResource.class.getPackage().getImplementationTitle();
    private static final String PRODUCT_VERSION = RecoveryXAResource.class.getPackage().getImplementationVersion();

    private final XAResource wrappedResource;
    private final String jndiName;
    private XAConnection xaConnection;

    public RecoveryXAResource(XAConnection connection, String name) throws SQLException {
        xaConnection = connection;
        wrappedResource = connection.getXAResource();
        jndiName = name;
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        if ( xaConnection == null ) {
            throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL );
        }
        Xid[] value = wrappedResource.recover( flag );
        if ( flag == TMENDRSCAN && ( value == null || value.length == 0 ) ) {
            close();
        }
        return value;
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        wrappedResource.commit( xid, onePhase );
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        wrappedResource.end( xid, flags );
    }

    @Override
    public void forget(Xid xid) throws XAException {
        wrappedResource.forget( xid );
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return wrappedResource.getTransactionTimeout();
    }

    @Override
    public boolean isSameRM(XAResource xares) throws XAException {
        return wrappedResource.isSameRM( xares );
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        return wrappedResource.prepare( xid );
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        wrappedResource.rollback( xid );
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
        return wrappedResource.setTransactionTimeout( seconds );
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
        wrappedResource.start( xid, flags );
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
        }
    }

    // --- //

    @Override
    public XAResource getResource() {
        return wrappedResource;
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
