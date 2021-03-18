// Copyright (C) 2018 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.exceptionsorter;

import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ExceptionSorter;

import java.sql.SQLException;

/**
 * Exception sorter for MySQL / MariaDB databases.
 *
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class MySQLExceptionSorter implements ExceptionSorter {

    @Override
    @SuppressWarnings( "MagicNumber" )
    public boolean isFatal(SQLException e) {
        if ( e.getSQLState() != null && e.getSQLState().startsWith( "08" ) ) {
            return true;
        }

        switch ( e.getErrorCode() ) {
            // Communications Errors
            case 1040: // ER_CON_COUNT_ERROR
            case 1042: // ER_BAD_HOST_ERROR
            case 1043: // ER_HANDSHAKE_ERROR
            case 1047: // ER_UNKNOWN_COM_ERROR
            case 1081: // ER_IPSOCK_ERROR
            case 1129: // ER_HOST_IS_BLOCKED
            case 1130: // ER_HOST_NOT_PRIVILEGED

            // Authentication Errors
            case 1045: // ER_ACCESS_DENIED_ERROR

            // Resource errors
            case 1004: // ER_CANT_CREATE_FILE
            case 1005: // ER_CANT_CREATE_TABLE
            case 1015: // ER_CANT_LOCK
            case 1021: // ER_DISK_FULL
            case 1041: // ER_OUT_OF_RESOURCES

            // Out-of-memory errors
            case 1037: // ER_OUTOFMEMORY
            case 1038: // ER_OUT_OF_SORTMEMORY
                return true;

            // All other errors
            default:
                return false;
        }
    }
}
