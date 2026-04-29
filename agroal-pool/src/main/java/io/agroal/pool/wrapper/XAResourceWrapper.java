package io.agroal.pool.wrapper;

import io.agroal.pool.ConnectionHandler;
import io.agroal.pool.util.AutoCloseableElement;
import io.agroal.pool.wrapper.closed.ClosedXAResource;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class XAResourceWrapper extends AutoCloseableElement<XAResourceWrapper> implements XAResource {

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
        return wrappedXAResource == ClosedXAResource.INSTANCE;
    }

    @Override
    public boolean isClosed() throws Exception {
        return internalClosed();
    }

    @Override
    public void close() throws Exception {
        handler.traceConnectionOperation( "xaResource.close()" );
        if ( wrappedXAResource != ClosedXAResource.INSTANCE ) {
            wrappedXAResource = ClosedXAResource.INSTANCE;
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
