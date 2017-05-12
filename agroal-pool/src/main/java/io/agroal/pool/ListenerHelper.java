// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.AgroalDataSourceListener;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class ListenerHelper {

    private ListenerHelper() {
    }

    public static void fireBeforeConnectionCreation(DataSource dataSource) {
        fire( dataSource.listenerList(), AgroalDataSourceListener::beforeConnectionCreation );
    }

    public static void fireOnConnectionCreation(DataSource dataSource, ConnectionHandler handler) {
        fire( dataSource.listenerList(), l -> l.onConnectionCreation( handler.getConnection() ) );
    }

    public static void fireBeforeConnectionAcquire(DataSource dataSource) {
        fire( dataSource.listenerList(), AgroalDataSourceListener::beforeConnectionAcquire );
    }

    public static void fireOnConnectionAcquired(DataSource dataSource, ConnectionHandler handler) {
        fire( dataSource.listenerList(), l -> l.onConnectionAcquire( handler.getConnection() ) );
    }

    public static void fireBeforeConnectionReturn(DataSource dataSource, ConnectionHandler handler) {
        fire( dataSource.listenerList(), l -> l.beforeConnectionReturn( handler.getConnection() ) );
    }

    public static void fireOnConnectionReturn(DataSource dataSource, ConnectionHandler handler) {
        fire( dataSource.listenerList(), l -> l.onConnectionReturn( handler.getConnection() ) );
    }

    public static void fireBeforeConnectionLeak(DataSource dataSource, ConnectionHandler handler) {
        fire( dataSource.listenerList(), l -> l.beforeConnectionLeak( handler.getConnection() ) );
    }

    public static void fireOnConnectionLeak(DataSource dataSource, ConnectionHandler handler) {
        fire( dataSource.listenerList(), l -> l.onConnectionLeak( handler.getConnection(), handler.getHoldingThread() ) );
    }

    public static void fireBeforeConnectionValidation(DataSource dataSource, ConnectionHandler handler) {
        fire( dataSource.listenerList(), l -> l.beforeConnectionValidation( handler.getConnection() ) );
    }

    public static void fireOnConnectionValid(DataSource dataSource, ConnectionHandler handler) {
        fire( dataSource.listenerList(), l -> l.onConnectionValid( handler.getConnection() ) );
    }

    public static void fireOnConnectionInvalid(DataSource dataSource, ConnectionHandler handler) {
        fire( dataSource.listenerList(), l -> l.onConnectionInvalid( handler.getConnection() ) );
    }

    public static void fireBeforeConnectionFlush(DataSource dataSource, ConnectionHandler handler) {
        fire( dataSource.listenerList(), l -> l.beforeConnectionFlush( handler.getConnection() ) );
    }

    public static void fireOnConnectionFlush(DataSource dataSource, ConnectionHandler handler) {
        fire( dataSource.listenerList(), l -> l.onConnectionFlush( handler.getConnection() ) );
    }

    public static void fireBeforeConnectionReap(DataSource dataSource, ConnectionHandler handler) {
        fire( dataSource.listenerList(), l -> l.beforeConnectionReap( handler.getConnection() ) );
    }

    public static void fireOnConnectionReap(DataSource dataSource, ConnectionHandler handler) {
        fire( dataSource.listenerList(), l -> l.onConnectionReap( handler.getConnection() ) );
    }

    public static void fireBeforeConnectionDestroy(DataSource dataSource, ConnectionHandler handler) {
        fire( dataSource.listenerList(), l -> l.beforeConnectionDestroy( handler.getConnection() ) );
    }

    public static void fireOnConnectionDestroy(DataSource dataSource, ConnectionHandler handler) {
        fire( dataSource.listenerList(), l -> l.onConnectionDestroy( handler.getConnection() ) );
    }

    public static void fireOnWarning(DataSource dataSource, Throwable throwable) {
        fire( dataSource.listenerList(), l -> l.onWarning( throwable ) );
    }

    private static void fire(Collection<AgroalDataSourceListener> listeners, Consumer<AgroalDataSourceListener> consumer) {
        for ( AgroalDataSourceListener listener : listeners ) {
            consumer.accept( listener );
        }
    }
}
