package io.agroal.pool;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static java.lang.System.nanoTime;
import static java.text.MessageFormat.format;
import static java.time.Duration.ZERO;
import static java.time.Duration.ofNanos;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class DefaultMetricsRepository implements MetricsRepository {

    private final ConnectionPool connectionPool;
    private final LongAdder creationCount = new LongAdder();
    private final LongAdder creationTotalTime = new LongAdder();
    private final LongAdder acquireCount = new LongAdder();
    private final LongAdder returnCount = new LongAdder();
    private final LongAdder acquireTotalTime = new LongAdder();
    private final LongAdder leakDetectionCount = new LongAdder();
    private final LongAdder invalidCount = new LongAdder();
    private final LongAdder flushCount = new LongAdder();
    private final LongAdder reapCount = new LongAdder();
    private final LongAdder destroyCount = new LongAdder();
    private final AtomicLong maxCreatedDuration = new AtomicLong( 0 );
    private final AtomicLong maxAcquireDuration = new AtomicLong( 0 );

    public DefaultMetricsRepository(ConnectionPool pool) {
        this.connectionPool = pool;
    }

    private void setMaxValue(AtomicLong field, long value) {
        for ( long oldMax; value > ( oldMax = field.get() ); ) {
            if ( field.compareAndSet( oldMax, value ) ) {
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
        setMaxValue( maxCreatedDuration, duration );
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
        setMaxValue( maxAcquireDuration, duration );
    }

    @Override
    public void afterConnectionReturn() {
        returnCount.increment();
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
    public Duration creationTimeAverage() {
        if ( creationCount.longValue() == 0 ) {
            return ZERO;
        }
        return ofNanos( creationTotalTime.longValue() / creationCount.longValue() );
    }

    @Override
    public Duration creationTimeMax() {
        return ofNanos( maxCreatedDuration.get() );
    }

    @Override
    public Duration creationTimeTotal() {
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
    public Duration blockingTimeAverage() {
        if ( acquireCount.longValue() == 0 ) {
            return ZERO;
        }
        return ofNanos( acquireTotalTime.longValue() / acquireCount.longValue() );
    }

    @Override
    public Duration blockingTimeMax() {
        return ofNanos( maxAcquireDuration.get() );
    }

    @Override
    public Duration blockingTimeTotal() {
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

        maxCreatedDuration.set( 0 );
        maxAcquireDuration.set( 0 );
        connectionPool.resetMaxUsedCount();
    }

    // --- //

    @Override
    public String toString() {
        double avgCreationMs = (double) creationTimeAverage().toNanos() / MILLISECONDS.toNanos( 1 );
        double avgBlockingMs = (double) blockingTimeAverage().toNanos() / MILLISECONDS.toNanos( 1 );

        String s1 = format( "Connections: {0} created | {1} invalid | {2} reap | {3} flush | {4} destroyed", creationCount, invalidCount, reapCount, flushCount, destroyCount );
        String s2 = format( "Pool: {0} available | {1} active | {2} max | {3} acquired | {4} returned", availableCount(), activeCount(), maxUsedCount(), acquireCount, returnCount );
        String s3 = format( "Created duration: {0,number,000.000}ms average | {1}ms max | {2}ms total", avgCreationMs, creationTimeMax().toMillis(), creationTimeTotal().toMillis() );
        String s4 = format( "Acquire duration: {0,number,000.000}ms average | {1}ms max | {2}ms total", avgBlockingMs, blockingTimeMax().toMillis(), blockingTimeTotal().toMillis() );
        String s5 = format( "Threads awaiting: {0}", awaitingCount() );

        String nl = System.lineSeparator();
        return nl + "===" + nl + s1 + nl + s2 + nl + s3 + nl + s4 + nl + s5 + nl + "===";
    }
}
