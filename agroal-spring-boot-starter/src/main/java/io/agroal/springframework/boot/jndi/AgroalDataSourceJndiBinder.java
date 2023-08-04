// Copyright (C) 2023 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.springframework.boot.jndi;

import io.agroal.api.AgroalDataSource;

@FunctionalInterface
public interface AgroalDataSourceJndiBinder {

    /**
     * Bind DataSource to JNDI returning true if successful.
     */
    public boolean bindToJndi(String jndiName, AgroalDataSource dataSource);
}
