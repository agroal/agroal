// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface AgroalDataSourceConfiguration {

    @Deprecated
    String jndiName();

    AgroalConnectionPoolConfiguration connectionPoolConfiguration();

    DataSourceImplementation dataSourceImplementation();

    boolean isXA();

    // --- //

    boolean metricsEnabled();

    void setMetricsEnabled(boolean metricsEnabled);

    void registerMetricsEnabledListener(MetricsEnabledListener metricsEnabledListener);

    // --- //

    enum DataSourceImplementation {

        AGROAL( "io.agroal.pool.DataSource" ),
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
