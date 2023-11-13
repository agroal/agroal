package io.agroal.narayana;

import javax.transaction.xa.XAException;

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
}
