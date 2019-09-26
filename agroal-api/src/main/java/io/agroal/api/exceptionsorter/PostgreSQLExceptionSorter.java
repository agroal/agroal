// Copyright (C) 2018 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.exceptionsorter;

import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ExceptionSorter;

import java.sql.SQLException;

/**
 * Exception sorter for PostgreSQL databases.
 *
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class PostgreSQLExceptionSorter implements ExceptionSorter {

    @Override
    public boolean isFatal(SQLException e) {
        return "08006".equals( e.getSQLState() );
    }
}
