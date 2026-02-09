package io.agroal.pool.wrapper;

import io.agroal.pool.ConnectionHandler;
import io.agroal.pool.util.AutoCloseableElement;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.lang.reflect.InvocationHandler;
import java.sql.SQLException;

import static java.lang.reflect.Proxy.newProxyInstance;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class XAResourceWrapper extends AutoCloseableElement<XAResourceWrapper> implements XAResource {

    private static final String CLOSED_HANDLER_STRING = XAResourceWrapper.class.getSimpleName() + ".CLOSED_XA_RESOURCE";

    private static final InvocationHandler CLOSED_HANDLER = (proxy, method, args) -> {
        switch ( method.getName() ) {
            case "close":
                return Void.TYPE;
            case "isClosed":
                return Boolean.TRUE;
            case "toString":
                return CLOSED_HANDLER_STRING;
            default:
                throw new SQLException( "XAConnection for the XAResource is closed" );
        }
    };

    private static final XAResource CLOSED_XA_RESOURCE = (XAResource) newProxyInstance( XAResource.class.getClassLoader(), new Class[]{XAResource.class}, CLOSED_HANDLER );

    // --- //

    private final ConnectionHandler handler;
    private XAResource wrappedXAResource;

    public XAResourceWrapper(ConnectionHandler connectionHandler, XAResource resource, AutoCloseableElement<XAResourceWrapper> head) {
        super( head );
        handler = connectionHandler;
        wrappedXAResource = resource;
    }
    
    @Override
    protected boolean internalClosed() {
        return wrappedXAResource == CLOSED_XA_RESOURCE;
    }

    @Override
    public boolean isClosed() throws Exception {
        return internalClosed();
    }

    @Override
    public void close() throws Exception {
        handler.traceConnectionOperation( "xaResource.close()" );
        if ( wrappedXAResource != CLOSED_XA_RESOURCE ) {
            wrappedXAResource = CLOSED_XA_RESOURCE;
        }
    }

    // --- //

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        handler.traceConnectionOperation( "xaResource.commit()" );
        wrappedXAResource.commit( xid, onePhase );
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        handler.traceConnectionOperation( "xaResource.end()" );
        wrappedXAResource.end( xid, flags );
    }

    @Override
    public void forget(Xid xid) throws XAException {
        handler.traceConnectionOperation( "xaResource.forget()" );
        wrappedXAResource.forget( xid );
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        handler.traceConnectionOperation( "xaResource.getTransactionTimeout()" );
        return wrappedXAResource.getTransactionTimeout();
    }

    @Override
    public boolean isSameRM(XAResource xares) throws XAException {
        handler.traceConnectionOperation( "xaResource.isSameRM()" );
        return wrappedXAResource.isSameRM( xares );
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        handler.traceConnectionOperation( "xaResource.prepare()" );
        return wrappedXAResource.prepare( xid );
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        handler.traceConnectionOperation( "xaResource.recover()" );
        return wrappedXAResource.recover( flag );
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        handler.traceConnectionOperation( "xaResource.rollback()" );
        wrappedXAResource.rollback( xid );
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
        handler.traceConnectionOperation( "xaResource.setTransactionTimeout()" );
        return wrappedXAResource.setTransactionTimeout( seconds );
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
        handler.traceConnectionOperation( "xaResource.start()" );
        wrappedXAResource.start( xid, flags );
    }
}
