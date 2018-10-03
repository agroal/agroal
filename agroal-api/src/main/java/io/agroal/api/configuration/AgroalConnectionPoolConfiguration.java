// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration;

import io.agroal.api.transaction.TransactionIntegration;

import java.sql.Connection;
import java.time.Duration;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface AgroalConnectionPoolConfiguration {

    AgroalConnectionFactoryConfiguration connectionFactoryConfiguration();

    ConnectionValidator connectionValidator();

    TransactionIntegration transactionIntegration();

    Duration leakTimeout();

    Duration validationTimeout();

    Duration reapTimeout();

    int initialSize();

    // --- Mutable attributes //

    int minSize();

    void setMinSize(int size);

    int maxSize();

    void setMaxSize(int size);

    Duration acquisitionTimeout();

    void setAcquisitionTimeout(Duration timeout);

    // --- //

    interface ConnectionValidator {

        static ConnectionValidator defaultValidator() {
            return new ConnectionValidator() {
                @Override
                public boolean isValid(Connection connection) {
                    try {
                        return connection.isValid( 0 );
                    } catch ( Exception t ) {
                        return false;
                    }
                }
            };
        }

        static ConnectionValidator emptyValidator() {
            return new ConnectionValidator() {
                @Override
                public boolean isValid(Connection connection) {
                    return true;
                }
            };
        }

        // --- //

        boolean isValid(Connection connection);
    }
}
