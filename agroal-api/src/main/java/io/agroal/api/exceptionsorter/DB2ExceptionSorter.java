// Copyright (C) 2018 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.exceptionsorter;

import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ExceptionSorter;

import java.sql.SQLException;

/**
 * DB2
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class DB2ExceptionSorter implements ExceptionSorter {

    /**
     * Constructor
     */
    public DB2ExceptionSorter() {
    }

    @Override
    public boolean isFatal(SQLException e) {
        int code = Math.abs(e.getErrorCode());
        boolean isFatal = false;
      
        if (code == 4470) {
            isFatal = true;
        } else if (code == 4499) {
            isFatal = true;
        }
      
        return isFatal;
    }
}
