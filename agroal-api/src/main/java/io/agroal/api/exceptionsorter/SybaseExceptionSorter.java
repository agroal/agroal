// Copyright (C) 2018 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.exceptionsorter;

import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ExceptionSorter;

import java.sql.SQLException;
import java.util.Locale;

/**
 * Exception sorter for Sybase databases.
 *
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class SybaseExceptionSorter implements ExceptionSorter {

    @Override
    public boolean isFatal(SQLException e) {
        String errorText = ( e.getMessage() ).toUpperCase( Locale.US );

        //     ERR_CONNECTION_DEAD              ERR_IOE_KILLED_CONNECTION        CONNECTION CLOSED
        return errorText.contains( "JZ0C0" ) || errorText.contains( "JZ0C1" ) || errorText.contains( "JZ006" );
    }
}
