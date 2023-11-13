package io.agroal.test.narayana;

import io.agroal.narayana.BaseXAResource;
import io.agroal.narayana.XAExceptionUtils;
import io.agroal.test.MockXAResource;
import jakarta.transaction.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.fail;

@Tag( FUNCTIONAL )
@Tag( TRANSACTION )
class XAExceptionTests {

    @DisplayName( "Test that XAException with XA_RB* on end() performs a rollback" )
    @ParameterizedTest
    @ValueSource( ints = {
            XAException.XA_RBROLLBACK, XAException.XA_RBCOMMFAIL, XAException.XA_RBDEADLOCK,
            XAException.XA_RBINTEGRITY, XAException.XA_RBPROTO, XAException.XA_RBTIMEOUT,
            XAException.XA_RBTRANSIENT} )
    void testEndWithRollback( int rbCode ) throws SystemException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();

        XAException endXAException = XAExceptionUtils.xaException( rbCode );
        try {
            txManager.begin();
            txManager.getTransaction().enlistResource( new BaseXAResource(new MockTransactionAware.Empty(), new EndThrows( endXAException ), null ) );
            txManager.getTransaction().enlistResource( new MockXAResource.Empty() ); // Force two phase commit
            txManager.commit();

            fail( "commit() should have thrown a RollbackException" );
        } catch ( RollbackException e ) {
            assertNotNull( e.getSuppressed() );
            assertTrue( e.getSuppressed().length == 1 );
            assertSame( endXAException, e.getSuppressed()[0] );
        } catch ( Exception e ) {
            fail( "Exception: " + e.getMessage() );
        } finally {
            try {
                txManager.rollback();
            } catch ( IllegalStateException e ) {
                // ignore
            }
        }
    }

    private static class EndThrows implements MockXAResource {
        private final XAException endXAException;

        public EndThrows( XAException endXAException ) {
            this.endXAException = endXAException;
        }

        @Override
        public void end( Xid xid, int i ) throws XAException {
            throw endXAException;
        }
    }
}
