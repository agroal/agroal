// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.springframework.boot.metrics;

import java.util.concurrent.TimeUnit;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceMetrics;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.binder.MeterBinder;

public class AgroalDataSourcePoolMetrics implements MeterBinder {

    public static final String AGROAL_METRIC_NAME_PREFIX = "agroal";
    private static final String METRIC_CATEGORY = "pool";
    private static final String METRIC_CREATION_COUNT = AGROAL_METRIC_NAME_PREFIX + ".connections.creation.count";
    private static final String METRIC_CREATION_TIME_AVG = AGROAL_METRIC_NAME_PREFIX + ".connections.creation.time.average";
    private static final String METRIC_CREATION_TIME_MAX = AGROAL_METRIC_NAME_PREFIX + ".connections.creation.time.max";
    private static final String METRIC_CREATION_TIME_TOTAL = AGROAL_METRIC_NAME_PREFIX + ".connections.creation.time.total";
    private static final String METRIC_LEAK_DETECTION_COUNT = AGROAL_METRIC_NAME_PREFIX + ".leak.detection.count";
    private static final String METRIC_INVALID_COUNT = AGROAL_METRIC_NAME_PREFIX + ".connections.invalid.count";
    private static final String METRIC_FLUSH_COUNT = AGROAL_METRIC_NAME_PREFIX + ".connections.flush.count";
    private static final String METRIC_REAP_COUNT = AGROAL_METRIC_NAME_PREFIX + ".connections.reap.count";
    private static final String METRIC_DESTROY_COUNT = AGROAL_METRIC_NAME_PREFIX + ".connections.destroy.count";
    private static final String METRIC_ACTIVE_COUNT = AGROAL_METRIC_NAME_PREFIX + ".connections.active.count";
    private static final String METRIC_MAX_USED_COUNT = AGROAL_METRIC_NAME_PREFIX + ".connections.max.used.count";
    private static final String METRIC_AVAILABLE_COUNT = AGROAL_METRIC_NAME_PREFIX + ".connections.available.count";
    private static final String METRIC_ACQUIRE_COUNT = AGROAL_METRIC_NAME_PREFIX + ".acquire.count";
    private static final String METRIC_BLOCKING_TIME_AVG = AGROAL_METRIC_NAME_PREFIX + ".blocking.time.average";
    private static final String METRIC_BLOCKING_TIME_MAX = AGROAL_METRIC_NAME_PREFIX + ".blocking.time.max";
    private static final String METRIC_BLOCKING_TIME_TOTAL = AGROAL_METRIC_NAME_PREFIX + ".blocking.time.total";
    private static final String METRIC_AWAITING_COUNT = AGROAL_METRIC_NAME_PREFIX + ".awaiting.count";

    private final String name;
    private final AgroalDataSource dataSource;

    public AgroalDataSourcePoolMetrics(String name, AgroalDataSource dataSource) {
        this.name = name;
        this.dataSource = dataSource;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        AgroalDataSourceMetrics metrics = dataSource.getMetrics();
        Gauge.builder(METRIC_CREATION_COUNT, metrics, AgroalDataSourceMetrics::creationCount)
                .description("Number of created connections")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
        TimeGauge.builder(METRIC_CREATION_TIME_AVG, () -> metrics.creationTimeAverage().toMillis(), TimeUnit.MILLISECONDS)
                .description("Average time for a connection to be created")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
        TimeGauge.builder(METRIC_CREATION_TIME_MAX, () -> metrics.creationTimeMax().toMillis(), TimeUnit.MILLISECONDS)
                .description("Maximum time for a connection to be created")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
        TimeGauge.builder(METRIC_CREATION_TIME_TOTAL, () -> metrics.creationTimeTotal().toMillis(), TimeUnit.MILLISECONDS)
                .description("Total time waiting for a connections to be created")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
        Gauge.builder(METRIC_LEAK_DETECTION_COUNT, metrics, AgroalDataSourceMetrics::leakDetectionCount)
                .description("Number of times a leak was detected")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
        Gauge.builder(METRIC_INVALID_COUNT, metrics, AgroalDataSourceMetrics::invalidCount)
                .description("Number of connections removed from the pool for being invalid")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
        Gauge.builder(METRIC_FLUSH_COUNT, metrics, AgroalDataSourceMetrics::flushCount)
                .description("Number of connections removed from the pool, not counting invalid / idle")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
        Gauge.builder(METRIC_REAP_COUNT, metrics, AgroalDataSourceMetrics::reapCount)
                .description("Number of connections removed from the pool for being idle")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
        Gauge.builder(METRIC_DESTROY_COUNT, metrics, AgroalDataSourceMetrics::destroyCount)
                .description("Number of destroyed connections")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
        Gauge.builder(METRIC_ACTIVE_COUNT, metrics, AgroalDataSourceMetrics::activeCount)
                .description("Number of active connections")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
        Gauge.builder(METRIC_MAX_USED_COUNT, metrics, AgroalDataSourceMetrics::maxUsedCount)
                .description("Maximum number of connections active simultaneously")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
        Gauge.builder(METRIC_AVAILABLE_COUNT, metrics, AgroalDataSourceMetrics::availableCount)
                .description("Number of idle connections in the pool, available to be acquired")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
        Gauge.builder(METRIC_ACQUIRE_COUNT, metrics, AgroalDataSourceMetrics::acquireCount)
                .description("Number of times an acquire operation succeeded")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
        TimeGauge.builder(METRIC_BLOCKING_TIME_AVG, () -> metrics.blockingTimeAverage().toMillis(), TimeUnit.MILLISECONDS)
                .description("Average time an application waited to acquire a connection")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
        TimeGauge.builder(METRIC_BLOCKING_TIME_MAX, () -> metrics.blockingTimeMax().toMillis(), TimeUnit.MILLISECONDS)
                .description("Maximum time an application waited to acquire a connection")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
        TimeGauge.builder(METRIC_BLOCKING_TIME_TOTAL, () -> metrics.blockingTimeTotal().toMillis(), TimeUnit.MILLISECONDS)
                .description("Total time applications waited to acquire a connection")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
        Gauge.builder(METRIC_AWAITING_COUNT, metrics, AgroalDataSourceMetrics::awaitingCount)
                .description("Approximate number of threads blocked, waiting to acquire a connection")
                .tags(METRIC_CATEGORY, name)
                .register(registry);
    }
}
