// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import io.agroal.api.AgroalDataSourceListener;
import io.agroal.pool.ConnectionHandler;

import java.sql.Connection;
import java.util.Arrays;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@SuppressWarnings( "UtilityClass" )
public final class ListenerHelper {

    private ListenerHelper() {
    }

    public static void fireBeforeConnectionCreation(AgroalDataSourceListener[] listeners) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.beforeConnectionCreation();
        }
    }

    public static void fireOnConnectionCreation(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionCreation( handler.rawConnection() );
        }
    }

    public static void fireOnConnectionPooled(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionPooled( handler.rawConnection() );
        }
    }

    public static void fireBeforeConnectionAcquire(AgroalDataSourceListener[] listeners) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.beforeConnectionAcquire();
        }
    }

    public static void fireOnConnectionAcquired(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionAcquire( handler.rawConnection() );
        }
    }

    public static void fireBeforeConnectionReturn(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.beforeConnectionReturn( handler.rawConnection() );
        }
    }

    public static void fireOnConnectionReturn(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionReturn( handler.rawConnection() );
        }
    }

    public static void fireBeforeConnectionLeak(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.beforeConnectionLeak( handler.rawConnection() );
        }
    }

    @SuppressWarnings( {"StringConcatenation", "ObjectAllocationInLoop"} )
    public static void fireOnConnectionLeak(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            Connection connection = handler.rawConnection();
            listener.onConnectionLeak( connection, handler.getHoldingThread() );
            if ( handler.getAcquisitionStackTrace() != null ) {
                if ( handler.isEnlisted() ) {
                    listener.onInfo( "Leaked connection " + connection + " is enlisted. Please make sure the associated transaction completes." );
                } else {
                    listener.onInfo( "Leaked connection " + connection + " is not enlisted. To return it to the pool use the flush(LEAK) operation." );
                }
                listener.onInfo( "Leaked connection " + connection + " acquired at: " + Arrays.toString( handler.getAcquisitionStackTrace() ) );
            }
            if ( handler.getConnectionOperations() != null ) {
                listener.onInfo( "Operations executed on leaked connection " + connection + ": " + String.join( ", ", handler.getConnectionOperations() ) );
            }
            if ( handler.getLastOperationStackTrace() != null ) {
                listener.onInfo( "Stack trace of last executed operation on " + connection + ": " + Arrays.toString( handler.getLastOperationStackTrace() ) );
            }
            if ( handler.getConnectionOperations() != null && handler.getConnectionOperations().contains( "unwrap(Class<T>)" ) ) {
                listener.onWarning( "A possible cause for the leak of connection " + connection + " is a call to the unwrap() method. close() needs to be called on the connection object provided by the pool." );
            }
        }
    }

    public static void fireBeforeConnectionValidation(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.beforeConnectionValidation( handler.rawConnection() );
        }
    }

    public static void fireOnConnectionValid(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionValid( handler.rawConnection() );
        }
    }

    public static void fireOnConnectionInvalid(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionInvalid( handler.rawConnection() );
        }
    }

    public static void fireBeforeConnectionFlush(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.beforeConnectionFlush( handler.rawConnection() );
        }
    }

    public static void fireOnConnectionFlush(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionFlush( handler.rawConnection() );
        }
    }

    public static void fireBeforeConnectionReap(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.beforeConnectionReap( handler.rawConnection() );
        }
    }

    public static void fireOnConnectionReap(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionReap( handler.rawConnection() );
        }
    }

    public static void fireBeforeConnectionDestroy(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.beforeConnectionDestroy( handler.rawConnection() );
        }
    }

    public static void fireOnConnectionDestroy(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionDestroy( handler.rawConnection() );
        }
    }

    public static void fireOnWarning(AgroalDataSourceListener[] listeners, String message) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onWarning( message );
        }
    }

    public static void fireOnWarning(AgroalDataSourceListener[] listeners, Throwable throwable) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onWarning( throwable );
        }
    }

    public static void fireOnInfo(AgroalDataSourceListener[] listeners, String message) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onInfo( message );
        }
    }

    public static void fireOnDebug(AgroalDataSourceListener[] listeners, String message) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onDebug( message );
        }
    }
}
