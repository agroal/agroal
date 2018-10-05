// Copyright (C) 2018 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.exceptionsorter;

import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ExceptionSorter;

import java.sql.SQLException;
import java.util.Locale;

/**
 * Sybase
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class SybaseExceptionSorter implements ExceptionSorter {

    /**
     * Constructor
     */
    public SybaseExceptionSorter() {
    }

    @Override
    public boolean isFatal(SQLException e) {
        boolean result = false;

        String errorText = (e.getMessage()).toUpperCase(Locale.US);
      
        if ((errorText.indexOf("JZ0C0") > -1) ||  // ERR_CONNECTION_DEAD
            (errorText.indexOf("JZ0C1") > -1) ||  // ERR_IOE_KILLED_CONNECTION
            (errorText.indexOf("JZ006") > -1)) {  // CONNECTION CLOSED
            result = true;
        }
      
        return result;
    }
}
