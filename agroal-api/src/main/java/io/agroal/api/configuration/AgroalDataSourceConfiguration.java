// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration;

/**
 * Configuration of an AgroalDataSource.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface AgroalDataSourceConfiguration {

    /**
     * Configuration of the pool of this DataSource
     */
    AgroalConnectionPoolConfiguration connectionPoolConfiguration();

    /**
     * Actual AgroalDataSource implementation. This allows flexibility to have different implementations of the Agroal API.
     */
    DataSourceImplementation dataSourceImplementation();

    // --- //

    /**
     * If this AgroalDataSource collects metrics.
     */
    boolean metricsEnabled();

    /**
     * Enables or disables the collection of metrics.
     */
    void setMetricsEnabled(boolean metricsEnabled);

    /**
     * Allows registration of a callback to be invoked each time there is a change on the status of metrics collection.
     */
    void registerMetricsEnabledListener(MetricsEnabledListener metricsEnabledListener);

    // --- //

    /**
     * Available implementations of AgroalDataSource.
     */
    enum DataSourceImplementation {

        /**
         * Agroal - the natural database connection pool!
         */
        AGROAL( "io.agroal.pool.DataSource" ),

        /**
         * Specialization of the Agroal pool for the flush-on-close use case.
         */
        AGROAL_POOLLESS( "io.agroal.pool.DataSource" ),

        /**
         * The popular Hikari connection pool. Mainly for testing purposes as the Agroal API is not fully supported.
         */
        HIKARI( "io.agroal.hikari.HikariUnderTheCovers" );

        private final String className;

        DataSourceImplementation(String className) {
            this.className = className;
        }

        public String className() {
            return className;
        }
    }

    // --- //

    interface MetricsEnabledListener {
        void onMetricsEnabled(boolean metricsEnabled);
    }
}
