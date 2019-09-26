// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api;

import io.agroal.api.configuration.AgroalDataSourceConfiguration;

/**
 * An interface for providers of AgroalDataSource.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface AgroalDataSourceProvider {

    /**
     * Factory method for AgroalDataSource. This method must return null if it can't create an AgroalDataSource based on the supplied configuration.
     */
    AgroalDataSource getDataSource(AgroalDataSourceConfiguration configuration, AgroalDataSourceListener... listeners);
}
