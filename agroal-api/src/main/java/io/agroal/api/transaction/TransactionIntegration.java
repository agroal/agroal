// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.transaction;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface TransactionIntegration {

    static TransactionIntegration none() {
        return new TransactionIntegration() {

            @Override
            public Connection getConnection() throws SQLException {
                return null;
            }

            @Override
            public void associate(Connection connection) throws SQLException {
            }

            @Override
            public boolean disassociate(Connection connection) throws SQLException {
                return true;
            }
        };
    }

    // --- //

    Connection getConnection() throws SQLException;

    void associate(Connection connection) throws SQLException;

    boolean disassociate(Connection connection) throws SQLException;
}
