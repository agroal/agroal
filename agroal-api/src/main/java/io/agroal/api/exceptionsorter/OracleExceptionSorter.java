// Copyright (C) 2018 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.exceptionsorter;

import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ExceptionSorter;

import java.sql.SQLException;
import java.util.Locale;

/**
 * Exception sorter for Oracle databases.
 *
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class OracleExceptionSorter implements ExceptionSorter {

    @Override
    public boolean isFatal(SQLException e) {
        int errorCode = Math.abs( e.getErrorCode() );

        if ( errorCode == 28             //session has been killed
                || errorCode == 600      //Internal oracle error
                || errorCode == 1012     //not logged on
                || errorCode == 1014     //Oracle shutdown in progress
                || errorCode == 1033     //Oracle initialization or shutdown in progress
                || errorCode == 1034     //Oracle not available
                || errorCode == 1035     //ORACLE only available to users with RESTRICTED SESSION privilege
                || errorCode == 1089     //immediate shutdown in progress - no operations are permitted
                || errorCode == 1090     //shutdown in progress - connection is not permitted
                || errorCode == 1092     //ORACLE instance terminated. Disconnection forced
                || errorCode == 1094     //ALTER DATABASE CLOSE in progress. Connections not permitted
                || errorCode == 2396     //exceeded maximum idle time, please connect again
                || errorCode == 3106     //fatal two-task communication protocol error
                || errorCode == 3111     //break received on communication channel
                || errorCode == 3113     //end-of-file on communication channel
                || errorCode == 3114     //not connected to ORACLE
                || errorCode >= 12100 && errorCode <= 12299    // TNS issues
                || errorCode == 17002    //connection reset
                || errorCode == 17008    //connection closed
                || errorCode == 17410    //No more data to read from socket
                || errorCode == 17447    //OALL8 is in an inconsistent state
        ) {
            return true;
        }

        // Exclude oracle user defined error codes (20000 through 20999) from consideration when looking for certain strings.
        String errorText = ( e.getMessage() ).toUpperCase( Locale.US );
        if ( ( errorCode < 20000 || errorCode >= 21000 ) && ( errorText.contains( "SOCKET" ) || errorText.contains( "CONNECTION HAS ALREADY BEEN CLOSED" ) || errorText.contains( "BROKEN PIPE" ) ) ) {
            return true;
        }

        return "08000".equals( e.getSQLState() );
    }
}
