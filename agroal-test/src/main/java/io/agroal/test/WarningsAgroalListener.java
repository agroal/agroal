// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test;

import io.agroal.api.AgroalDataSourceListener;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WarningsAgroalListener implements AgroalDataSourceListener {

    static final Logger logger = getLogger( WarningsAgroalListener.class.getName() );
    private final List<String> warnings = Collections.synchronizedList(new ArrayList<>());
    private final List<String> failures = Collections.synchronizedList(new ArrayList<>());


    @Override
    public void onWarning(String message) {
        warnings.add( message );
        logger.info( "Expected WARN: " + message );
    }

    @Override
    public void onWarning(Throwable throwable) {
        onWarning( stackTraceAsString(throwable) );
    }

    @Override
    public void onConnectionCreationFailure(SQLException sqlException) {
        var message = stackTraceAsString(sqlException);
        failures.add( message );
        logger.info( "Expected FAILURE: " + message );
    }

    public int warningCount() {
        return warnings.size();
    }

    public int failuresCount() {
        return failures.size();
    }

    public void assertAnyWarningStartsWith( String startOfMessage ) {
        var result = warnings.stream().anyMatch( w -> w.startsWith( startOfMessage ) );
        assertTrue(result);
    }

    public void assertAnyFailureStartsWith( String startOfMessage ) {
        var result = failures.stream().anyMatch( w -> w.startsWith( startOfMessage ) );
        assertTrue(result);
    }

    private static String stackTraceAsString( Throwable throwable ) {
        var s = new StringWriter();
        var p = new PrintWriter(s);
        throwable.printStackTrace(p);
        return s.toString();
    }

    public void assertNoConnectionFailures() {
        assertEquals(0, failuresCount());
    }

    public void reset() {
        warnings.clear();
        failures.clear();
    }
}

