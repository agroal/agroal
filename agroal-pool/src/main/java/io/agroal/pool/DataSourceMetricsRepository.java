// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.AgroalDataSourceMetrics;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;

import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.time.Duration.ofNanos;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface DataSourceMetricsRepository extends AgroalDataSourceMetrics {

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

    class EmptyMetricsRepository implements DataSourceMetricsRepository {

        @Override
        public String toString() {
            return "Metrics Disabled";
        }
    }

    // --- //

    class DefaultMetricsRepository implements DataSourceMetricsRepository {

        private static final AtomicLongFieldUpdater<DefaultMetricsRepository> maxCreated = newUpdater( DefaultMetricsRepository.class, "maxCreatedDuration" );
        private static final AtomicLongFieldUpdater<DefaultMetricsRepository> maxAcquire = newUpdater( DefaultMetricsRepository.class, "maxAcquireDuration" );
        private final ConnectionPool connectionPool;
        private final LongAdder creationCount = new LongAdder();
        private final LongAdder creationTotalTime = new LongAdder();
        private final LongAdder acquireCount = new LongAdder();
        private final LongAdder acquireTotalTime = new LongAdder();
        private final LongAdder leakDetectionCount = new LongAdder();
        private final LongAdder invalidCount = new LongAdder();
        private final LongAdder flushCount = new LongAdder();
        private final LongAdder reapCount = new LongAdder();
        private final LongAdder destroyCount = new LongAdder();
        private volatile long maxCreatedDuration = 0;
        private volatile long maxAcquireDuration = 0;

        public DefaultMetricsRepository(ConnectionPool pool) {
            this.connectionPool = pool;
        }

        private void setMaxValue(AtomicLongFieldUpdater<DefaultMetricsRepository> updater, long value) {
            for ( long oldMax; value > ( oldMax = updater.get( this ) ); ) {
                if ( updater.compareAndSet( this, oldMax, value ) ) {
                    return;
                }
            }
        }

        @Override
        public long beforeConnectionCreation() {
            return nanoTime();
        }

        @Override
        public void afterConnectionCreation(long timestamp) {
            long duration = nanoTime() - timestamp;
            creationCount.increment();
            creationTotalTime.add( duration );
            setMaxValue( maxCreated, duration );
        }

        @Override
        public long beforeConnectionAcquire() {
            return nanoTime();
        }

        @Override
        public void afterConnectionAcquire(long timestamp) {
            long duration = nanoTime() - timestamp;
            acquireCount.increment();
            acquireTotalTime.add( duration );
            setMaxValue( maxAcquire, duration );
        }

        @Override
        public void afterLeakDetection() {
            leakDetectionCount.increment();
        }

        @Override
        public void afterConnectionInvalid() {
            invalidCount.increment();
        }

        @Override
        public void afterConnectionFlush() {
            flushCount.increment();
        }

        @Override
        public void afterConnectionReap() {
            reapCount.increment();
        }

        @Override
        public void afterConnectionDestroy() {
            destroyCount.increment();
        }

        // --- //

        @Override
        public long creationCount() {
            return creationCount.longValue();
        }

        @Override
        public Duration averageCreationTime() {
            return ofNanos( creationTotalTime.longValue() / creationCount.longValue() );
        }

        @Override
        public Duration maxCreationTime() {
            return ofNanos( maxCreatedDuration );
        }

        @Override
        public Duration totalCreationTime() {
            return ofNanos( creationTotalTime.longValue() );
        }

        @Override
        public long acquireCount() {
            return acquireCount.longValue();
        }

        @Override
        public long leakDetectionCount() {
            return leakDetectionCount.longValue();
        }

        @Override
        public long invalidCount() {
            return invalidCount.longValue();
        }

        @Override
        public long flushCount() {
            return flushCount.longValue();
        }

        @Override
        public long reapCount() {
            return reapCount.longValue();
        }

        @Override
        public long destroyCount() {
            return destroyCount.longValue();
        }

        @Override
        public long activeCount() {
            return connectionPool.activeCount();
        }

        @Override
        public long maxUsedCount() {
            return connectionPool.maxUsedCount();
        }

        @Override
        public long availableCount() {
            return connectionPool.availableCount();
        }

        @Override
        public Duration averageBlockingTime() {
            return ofNanos( acquireTotalTime.longValue() / acquireCount.longValue() );
        }

        @Override
        public Duration maxBlockingTime() {
            return ofNanos( maxAcquireDuration );
        }

        @Override
        public Duration totalBlockingTime() {
            return ofNanos( acquireTotalTime.longValue() );
        }

        @Override
        public long awaitingCount() {
            return connectionPool.awaitingCount();
        }

        // --- //

        @Override
        public void reset() {
            creationCount.reset();
            creationTotalTime.reset();
            acquireCount.reset();
            acquireTotalTime.reset();
            leakDetectionCount.reset();
            invalidCount.reset();

            maxCreatedDuration = 0;
            maxAcquireDuration = 0;
            connectionPool.resetMaxUsedCount();
        }

        // --- //

        @Override
        public String toString() {
            double avgCreationMs = (double) averageCreationTime().toNanos() / MILLISECONDS.toNanos( 1 );
            double avgBlockingMs = (double) averageBlockingTime().toNanos() / MILLISECONDS.toNanos( 1 );

            String s1 = format( "Connections: %s created / %s acquired / %s destroyed / %s invalid / %s reap %n", creationCount, acquireCount, destroyCount, invalidCount, reapCount );
            String s2 = format( "Pool: %d available / %d active / %d max %n", availableCount(), activeCount(), maxUsedCount() );
            String s3 = format( "Created duration: %3.3fms average / %dms max / %dms total %n", avgCreationMs, maxCreationTime().toMillis(), totalCreationTime().toMillis() );
            String s4 = format( "Acquire duration: %3.3fms average / %dms max / %dms total %n", avgBlockingMs, maxBlockingTime().toMillis(), totalBlockingTime().toMillis() );
            String s5 = format( "Threads awaiting: %d %n", awaitingCount() );
            return s1 + s2 + s3 + s4 + s5;
        }
    }
}
