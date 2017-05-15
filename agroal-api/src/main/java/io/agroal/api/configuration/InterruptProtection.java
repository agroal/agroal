// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration;

import java.sql.SQLException;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Deprecated
public interface InterruptProtection {

    static InterruptProtection none() {
        return new InterruptProtection() {

            @Override
            public <T> T protect(SQLCallable<T> callable) throws SQLException {
                return callable.call();
            }
        };
    }

    default void protect(SQLRunnable runnable) throws SQLException {
        protect( () -> runnable );
    }

    <T> T protect(SQLCallable<T> callable) throws SQLException;

    // --- //

    @FunctionalInterface
    interface SQLCallable<T> {

        T call() throws SQLException;
    }

    @FunctionalInterface
    interface SQLRunnable {

        void run() throws SQLException;
    }
}
