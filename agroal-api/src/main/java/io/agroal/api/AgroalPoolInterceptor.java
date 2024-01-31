// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api;

import java.sql.Connection;
import java.util.Comparator;

/**
 * Callback interface for pool actions.
 * <p>
 * These differ from the Listener in a few different ways:
 * They do not have access to the raw Connection
 * The invoke order is dependent on the operation (incoming / outgoing)
 * Consistent with the transaction.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface AgroalPoolInterceptor {

    /**
     * Uses interceptor priority, followed by {@link Class#getName()} to ensure a consistent ordering.
     */
    Comparator<AgroalPoolInterceptor> DEFAULT_COMPARATOR = new Comparator<AgroalPoolInterceptor>() {
        @Override
        public int compare(AgroalPoolInterceptor i1, AgroalPoolInterceptor i2) {
            int value = Integer.compare( i1.getPriority(), i2.getPriority() );
            return value != 0 ? value : i1.getClass().getName().compareTo( i2.getClass().getName() );
        }
    };


    /**
     * This callback is invoked when a new connection is created.
     */
    default void onConnectionCreate(Connection connection) {
    }

    /**
     * This callback is invoked when a connection is successfully acquired.
     * When in a transactional environment this is invoked only once for multiple acquire calls within the same transaction, before the connection is associated.
     */
    default void onConnectionAcquire(Connection connection) {
    }

    /**
     * This callback is invoked before a connection is returned to the pool.
     * When in a transactional environment this is invoked only once for each transaction, after commit / rollback.
     * This callback runs after reset, allowing connections to be in the pool in a different state than the described on the configuration.
     */
    default void onConnectionReturn(Connection connection) {
    }


    /**
     * This callback is invoked just before a connection is destroyed.
     */
    default void onConnectionDestroy(Connection connection) {
    }


    /**
     * Allows a ordering between multiple interceptors.
     * Lower priority are executed first. Negative values are reserved.
     */
    default int getPriority() {
        return 0;
    }
}
