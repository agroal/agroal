// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api;

import java.time.Duration;

/**
 * Several metrics provided by the pool.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface AgroalDataSourceMetrics {

    /**
     * Number of created connections.
     */
    default long creationCount() {
        return 0;
    }

    /**
     * Average time for a connection to be created.
     */
    default Duration creationTimeAverage() {
        return Duration.ZERO;
    }

    /**
     * Maximum time for a connection to be created.
     */
    default Duration creationTimeMax() {
        return Duration.ZERO;
    }

    /**
     * Total time waiting for a connections to be created.
     */
    default Duration creationTimeTotal() {
        return Duration.ZERO;
    }

    /**
     * Number of times a leak was detected. A single connection can be detected multiple times.
     */
    default long leakDetectionCount() {
        return 0;
    }

    /**
     * Number of connections removed from the pool for being invalid.
     */
    default long invalidCount() {
        return 0;
    }

    /**
     * Number of connections removed from the pool, not counting invalid / idle.
     */
    default long flushCount() {
        return 0;
    }

    /**
     * Number of connections removed from the pool for being idle.
     */
    default long reapCount() {
        return 0;
    }

    /**
     * Number of destroyed connections.
     */
    default long destroyCount() {
        return 0;
    }

    // --- //

    /**
     * Number of active connections. This connections are in use and not available to be acquired.
     */
    default long activeCount() {
        return 0;
    }

    /**
     * Maximum number of connections active simultaneously.
     */
    default long maxUsedCount() {
        return 0;
    }

    /**
     * Number of idle connections in the pool, available to be acquired.
     */
    default long availableCount() {
        return 0;
    }

    /**
     * Number of times an acquire operation succeeded.
     */
    default long acquireCount() {
        return 0;
    }

    /**
     * Average time an application waited to acquire a connection.
     */
    default Duration blockingTimeAverage() {
        return Duration.ZERO;
    }

    /**
     * Maximum time an application waited to acquire a connection.
     */
    default Duration blockingTimeMax() {
        return Duration.ZERO;
    }

    /**
     * Total time applications waited to acquire a connection.
     */
    default Duration blockingTimeTotal() {
        return Duration.ZERO;
    }

    /**
     * Approximate number of threads blocked, waiting to acquire a connection.
     */
    default long awaitingCount() {
        return 0;
    }

    // --- //

    /**
     * Reset the metrics.
     */
    default void reset() {
    }
}
