// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration;

import java.sql.Connection;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface ConnectionValidator {
    
    static ConnectionValidator defaultValidator() {
        return connection -> {
            try {
                return connection.isValid( 0 );
            } catch ( Exception t ) {
                return false;
            }
        };
    }

    static ConnectionValidator emptyValidator() {
        return connection -> true;
    }

    // --- //

    boolean isValid(Connection connection);
}
