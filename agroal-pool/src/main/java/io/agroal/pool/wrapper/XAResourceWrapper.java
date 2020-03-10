package io.agroal.pool.wrapper;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.SQLException;

public class XAResourceWrapper implements XAResource {

    private final XAResource wrappedResource;
    private XAConnection xaConnection;

    public XAResourceWrapper(XAConnection connection) throws SQLException {
        xaConnection = connection;
        wrappedResource = connection.getXAResource();
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        if ( xaConnection == null ) {
            throw new XAException( XAException.XAER_RMFAIL );
        }
        Xid[] value = wrappedResource.recover( flag );
        if ( flag == TMENDRSCAN && ( value == null || value.length == 0 ) ) {
            try {
                xaConnection.close();
            } catch ( SQLException e ) {
                throw new XAException( e.getMessage() );
            } finally {
                xaConnection = null;
            }
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
}
