// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import io.agroal.api.AgroalDataSourceListener;
import io.agroal.pool.ConnectionHandler;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
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
            listener.onConnectionCreation( handler.getConnection() );
        }
    }

    public static void fireOnConnectionPooled(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionPooled( handler.getConnection() );
        }
    }

    public static void fireBeforeConnectionAcquire(AgroalDataSourceListener[] listeners) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.beforeConnectionAcquire();
        }
    }

    public static void fireOnConnectionAcquired(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionAcquire( handler.getConnection() );
        }
    }

    public static void fireBeforeConnectionReturn(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.beforeConnectionReturn( handler.getConnection() );
        }
    }

    public static void fireOnConnectionReturn(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionReturn( handler.getConnection() );
        }
    }

    public static void fireBeforeConnectionLeak(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.beforeConnectionLeak( handler.getConnection() );
        }
    }

    public static void fireOnConnectionLeak(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionLeak( handler.getConnection(), handler.getHoldingThread() );
        }
    }

    public static void fireBeforeConnectionValidation(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.beforeConnectionValidation( handler.getConnection() );
        }
    }

    public static void fireOnConnectionValid(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionValid( handler.getConnection() );
        }
    }

    public static void fireOnConnectionInvalid(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionInvalid( handler.getConnection() );
        }
    }

    public static void fireBeforeConnectionFlush(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.beforeConnectionFlush( handler.getConnection() );
        }
    }

    public static void fireOnConnectionFlush(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionFlush( handler.getConnection() );
        }
    }

    public static void fireBeforeConnectionReap(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.beforeConnectionReap( handler.getConnection() );
        }
    }

    public static void fireOnConnectionReap(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionReap( handler.getConnection() );
        }
    }

    public static void fireBeforeConnectionDestroy(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.beforeConnectionDestroy( handler.getConnection() );
        }
    }

    public static void fireOnConnectionDestroy(AgroalDataSourceListener[] listeners, ConnectionHandler handler) {
        for ( AgroalDataSourceListener listener : listeners ) {
            listener.onConnectionDestroy( handler.getConnection() );
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
}
