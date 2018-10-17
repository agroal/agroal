// Copyright (C) 2018 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.exceptionsorter;

import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ExceptionSorter;

import java.sql.SQLException;

/**
 * PostgreSQL
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class PostgreSQLExceptionSorter implements ExceptionSorter {

    /**
     * Constructor
     */
    public PostgreSQLExceptionSorter() {
    }

    @Override
    public boolean isFatal(SQLException e) {
        String sqlState = e.getSQLState();

        if ("08006".equals(sqlState)) {
            return true;
        }

        return false;
    }
}
