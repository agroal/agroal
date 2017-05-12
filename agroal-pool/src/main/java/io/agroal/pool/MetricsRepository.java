// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.AgroalDataSourceMetrics;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface MetricsRepository extends AgroalDataSourceMetrics {

    default long beforeConnectionCreation() {
        return 0;
    }

    default void afterConnectionCreation(long timestamp) {
    }

    default long beforeConnectionAcquire() {
        return 0;
    }

    default void afterConnectionAcquire(long timestamp) {
    }

    default void afterConnectionReturn() {
    }

    default void afterLeakDetection() {
    }

    default void afterConnectionInvalid() {
    }

    default void afterConnectionFlush() {
    }

    default void afterConnectionReap() {
    }

    default void afterConnectionDestroy() {
    }

    // --- //

    class EmptyMetricsRepository implements MetricsRepository {

        @Override
        public String toString() {
            return "Metrics Disabled";
        }
    }

    // --- //
}
