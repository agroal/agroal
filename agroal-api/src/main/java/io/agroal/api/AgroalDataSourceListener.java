// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api;

import java.sql.Connection;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface AgroalDataSourceListener {

    default void beforeConnectionCreation() {}

    default void onConnectionCreation(Connection connection) {}

    default void onConnectionPooled(Connection connection) {}

    default void beforeConnectionAcquire() {}

    default void onConnectionAcquire(Connection connection) {}

    default void beforeConnectionReturn(Connection connection) {}

    default void onConnectionReturn(Connection connection) {}

    default void beforeConnectionLeak(Connection connection) {}

    default void onConnectionLeak(Connection connection, Thread thread) {}

    default void beforeConnectionValidation(Connection connection) {}

    default void onConnectionValid(Connection connection) {}

    default void onConnectionInvalid(Connection connection) {}

    default void beforeConnectionFlush(Connection connection) {}

    default void onConnectionFlush(Connection connection) {}

    default void beforeConnectionReap(Connection connection) {}

    default void onConnectionReap(Connection connection) {}

    default void beforeConnectionDestroy(Connection connection) {}

    default void onConnectionDestroy(Connection connection) {}

    default void onWarning(String message) {}

    default void onWarning(Throwable throwable) {}

}
