// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api;

import java.time.Duration;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface AgroalDataSourceMetrics {

    default long creationCount() {
        return 0;
    }

    default Duration creationTimeAverage() {
        return Duration.ZERO;
    }

    default Duration creationTimeMax() {
        return Duration.ZERO;
    }

    default Duration creationTimeTotal() {
        return Duration.ZERO;
    }

    default long leakDetectionCount() {
        return 0;
    }

    default long invalidCount() {
        return 0;
    }

    default long flushCount() {
        return 0;
    }

    default long reapCount() {
        return 0;
    }

    default long destroyCount() {
        return 0;
    }

    // --- //

    default long activeCount() {
        return 0;
    }

    default long maxUsedCount() {
        return 0;
    }

    default long availableCount() {
        return 0;
    }

    default long acquireCount() {
        return 0;
    }

    default Duration blockingTimeAverage() {
        return Duration.ZERO;
    }

    default Duration blockingTimeMax() {
        return Duration.ZERO;
    }

    default Duration blockingTimeTotal() {
        return Duration.ZERO;
    }

    default long awaitingCount() {
        return 0;
    }

    // --- //

    default void reset() {
    }
}
