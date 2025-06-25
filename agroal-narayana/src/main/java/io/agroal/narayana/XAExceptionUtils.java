package io.agroal.narayana;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

public abstract class XAExceptionUtils {
    @SuppressWarnings( "StringConcatenation" )
    public static XAException xaException( int errorCode, String message, Throwable cause ) {
        XAException xaException = xaException( errorCode, message + cause.getMessage() );
        xaException.initCause( cause );
        return xaException;
    }

    public static XAException xaException( int errorCode, Throwable cause ) {
        XAException xaException = xaException( errorCode, cause.getMessage() );
        xaException.initCause( cause );
        return xaException;
    }

    public static XAException xaException( int errorCode, String message ) {
        XAException xaException = new XAException( message );
        xaException.errorCode = errorCode;
        return xaException;
    }

    public static XAException xaException( int errorCode ) {
        return new XAException( errorCode );
    }

    /**
     * Function that returns true if the returnCode is one of the Rollback-only codes and the flags match TMFAIL.
     * <p>
     * This function describes the TMFAIL case from page 38 of
     * <a href="https://pubs.opengroup.org/onlinepubs/009680699/toc.pdf">The XA Specification</a>:
     * <pre>
     * The portion of work has failed. A resource manager might choose to mark a
     * transaction branch as rollback-only at this point. In fact, a transaction manager
     * does so for the global transaction. If a resource manager chooses to do so also,
     * xa_end() returns one of the [XA_RB∗] values. TMFAIL cannot be used in
     * conjunction with either TMSUSPEND or TMSUCCESS
     * </pre>
     * @param returnCode returnCode that was returned by the RM
     * @param flags flags that were used to call the method on the XAResource
     * @return true is flags matches TMFAIL and returnCode is one of the defined  XA_RB* codes
     */
    public static boolean isUnilateralRollbackOnAbort(int returnCode, int flags) {
        return (flags & XAResource.TMFAIL) > 0 && returnCode >= XAException.XA_RBBASE && returnCode <= XAException.XA_RBEND;
    }
}
