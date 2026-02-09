package io.agroal.pool;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

import static java.lang.System.nanoTime;
import static java.time.Duration.ZERO;
import static java.time.Duration.ofNanos;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class DefaultMetricsRepository implements MetricsRepository {

    private static final String FORMAT_1 = "Connections: {0} created | {1} invalid | {2} reap | {3} flush | {4} destroyed";
    private static final String FORMAT_2 = "Pool: {0} available | {1} active | {2} max | {3} acquired | {4} returned";
    private static final String FORMAT_3 = "Created duration: {0,number,000.000}ms average | {1}ms max | {2}ms total";
    private static final String FORMAT_4 = "Acquire duration: {0,number,000.000}ms average | {1}ms max | {2}ms total";
    private static final String FORMAT_5 = "Threads awaiting: {0}";

    private final Pool connectionPool;
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
    private final LongAccumulator maxCreatedDuration = new LongAccumulator( Long::max, 0 );
    private final LongAccumulator maxAcquireDuration = new LongAccumulator( Long::max, 0 );

    public DefaultMetricsRepository(Pool pool) {
        connectionPool = pool;
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
        maxCreatedDuration.accumulate( duration );
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
        maxAcquireDuration.accumulate( duration );
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
    public long heldCount() {
        return connectionPool.heldCount();
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

        maxCreatedDuration.reset();
        maxAcquireDuration.reset();
        connectionPool.resetMaxUsedCount();
    }

    // --- //

    @Override
    public String toString() {
        double avgCreationMs = (double) creationTimeAverage().toNanos() / MILLISECONDS.toNanos( 1 );
        double avgBlockingMs = (double) blockingTimeAverage().toNanos() / MILLISECONDS.toNanos( 1 );

        String nl = System.lineSeparator();

        StringBuffer buffer = new StringBuffer( 500 );
        buffer.append( nl ).append( "===" ).append( nl );
        new MessageFormat( FORMAT_1, Locale.ROOT ).format( new Object[]{creationCount, invalidCount, reapCount, flushCount, destroyCount}, buffer, null ).append( nl );
        new MessageFormat( FORMAT_2, Locale.ROOT ).format( new Object[]{availableCount(), activeCount(), maxUsedCount(), acquireCount, returnCount}, buffer, null ).append( nl );
        new MessageFormat( FORMAT_3, Locale.ROOT ).format( new Object[]{avgCreationMs, creationTimeMax().toMillis(), creationTimeTotal().toMillis()}, buffer, null ).append( nl );
        new MessageFormat( FORMAT_4, Locale.ROOT ).format( new Object[]{avgBlockingMs, blockingTimeMax().toMillis(), blockingTimeTotal().toMillis()}, buffer, null ).append( nl );
        new MessageFormat( FORMAT_5, Locale.ROOT ).format( new Object[]{awaitingCount()}, buffer, null ).append( nl );
        return buffer.append( "===" ).toString();
    }
}
