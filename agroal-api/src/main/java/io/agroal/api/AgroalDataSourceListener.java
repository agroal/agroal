// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api;

import java.sql.Connection;

/**
 * This interface defines a set of callback methods that are invoked on events considered important for the state of the pool.
 * It not only allows for logging this events, but also for black-box testing.
 *
 * NOTE: implementations of this methods should not block / do expensive operations.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface AgroalDataSourceListener {

    /**
     * This callback is invoked whenever a new connection is about to be created.
     */
    default void beforeConnectionCreation() {}

    /**
     * This callback is invoked for every new connection.
     */
    default void onConnectionCreation(Connection connection) {}

    /**
     * This callback is invoked right after a connection is added to the pool. The connection may have been acquired concurrently.
     */
    default void onConnectionPooled(Connection connection) {}

    /**
     * This callback is invoked whenever an application tries to obtain a connection.
     */
    default void beforeConnectionAcquire() {}

    /**
     *  This callback is invoked when a connection is successfully acquired.
     */
    default void onConnectionAcquire(Connection connection) {}

    /**
     * This callback is invoked before a connection is returned to the pool.
     */
    default void beforeConnectionReturn(Connection connection) {}

    /**
     * This callback is invoked right after a connection is returned to the pool. The connection may have been acquired concurrently.
     */
    default void onConnectionReturn(Connection connection) {}

    /**
     * This callback is invoked before checking the leak timeout of a connection.
     */
    default void beforeConnectionLeak(Connection connection) {}

    /**
     * This connection is invoked when a connection is held for longer than the leak timeout and reports what thread acquired it.
     */
    default void onConnectionLeak(Connection connection, Thread thread) {}

    /**
     * This callback is invoked when a connection is about to be checked.
     */
    default void beforeConnectionValidation(Connection connection) {}

    /**
     * This callback is invoked when a connection was checked and is valid.
     */
    default void onConnectionValid(Connection connection) {}

    /**
     * This callback is invoked when a connection was checked and is invalid. The connection will be destroyed.
     */
    default void onConnectionInvalid(Connection connection) {}

    /**
     * This callback is invoked when a connection is about to be flush. It may not be flushed.
     */
    default void beforeConnectionFlush(Connection connection) {}

    /**
     * This callback is invoked when after a connection is removed from the pool.
     */
    default void onConnectionFlush(Connection connection) {}

    /**
     * This callback is invoked before checking the idle timeout of a connection.
     */
    default void beforeConnectionReap(Connection connection) {}

    /**
     * This callback is invoked if a connection is idle in the pool. The connection will be destroyed.
     */
    default void onConnectionReap(Connection connection) {}

    /**
     * This callback is invoked whenever a connection is about to be destroyed.
     */
    default void beforeConnectionDestroy(Connection connection) {}

    /**
     * This callback is invoked after a connection is closed.
     */
    default void onConnectionDestroy(Connection connection) {}

    /**
     * Callback is invoked for each pool interceptor that is installed.
     */
    default void onPoolInterceptor(AgroalPoolInterceptor interceptor) {}

    /**
     * This callback is invoked to report anomalous circumstances that do not prevent the pool from functioning.
     */
    default void onWarning(String message) {}

    /**
     * This callback is invoked to report anomalous circumstances that do not prevent the pool from functioning.
     */
    default void onWarning(Throwable throwable) {}

    /**
     * Callback to allow reporting information of interest, for which a warning might be considered excessive.
     */
    default void onInfo(String message) {}

}
