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
        connect( false );
    }

    private boolean connect(boolean flag) throws SQLException {
        if ( wrappedResource == null ) {
            xaConnection = connectionFactory.getRecoveryConnection();
            wrappedResource = xaConnection.getXAResource();
            return flag;
        }
        return false;
    }

    private <R> R getConnectedResource(Function<XAResource, R> f) throws XAException {
        boolean reconnect = false;
        try {
            reconnect = connect( true );
            return f.apply( wrappedResource );
        } catch ( SQLException e ) {
            throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, e );
        } finally {
            if ( reconnect ) {
                close();
            }
        }
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        try {
            if ( flag == TMSTARTRSCAN ) {
                connect(false);
            }
            Xid[] value = getConnectedResource( xaResource -> xaResource.recover( flag ) );
            if ( flag == TMENDRSCAN && ( value == null || value.length == 0 ) ) {
                close();
            }
            return value;
        } catch ( SQLException e ) {
            throw XAExceptionUtils.xaException( XAException.XAER_RMFAIL, e );
        }
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        getConnectedResource( xaResource -> { xaResource.commit( xid, onePhase ); return null; } );
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        getConnectedResource( xaResource -> { xaResource.end( xid, flags ); return null; } );
    }

    @Override
    public void forget(Xid xid) throws XAException {
        getConnectedResource( xaResource -> { xaResource.forget( xid ); return null; } );
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return getConnectedResource( XAResource::getTransactionTimeout );
    }

    @Override
    public boolean isSameRM(XAResource xares) throws XAException {
        return getConnectedResource( xaResource -> xaResource.isSameRM( xares ) );
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        return getConnectedResource( xaResource -> xaResource.prepare( xid ) );
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        getConnectedResource( xaResource -> { xaResource.rollback( xid ); return null; } );
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
        return getConnectedResource( xaResource -> xaResource.setTransactionTimeout( seconds ) );
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
        getConnectedResource( xaResource -> { xaResource.start( xid, flags ); return null; } );
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

    private interface Function<T, R> {
        R apply(T t) throws XAException;
    }
}
