// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.MockDriver;
import io.agroal.test.MySQLConnectMockDriver;
import io.agroal.test.WarningsAgroalListener;
import io.agroal.test.fakeserver.AcceptConnectionAndClose;
import io.agroal.test.fakeserver.AcceptConnectionAndDoNotRespond;
import io.agroal.test.fakeserver.ServerNotListening;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag( FUNCTIONAL )
public class NewConnectionTimeoutTests {

    static final Logger logger = getLogger( NewConnectionTimeoutTests.class.getName() );

    @BeforeAll
    static void setup(){
        if ( Utils.isWindowsOS() ) {
            Utils.windowsTimerHack();
        }
    }

    @BeforeEach
    void startClean() {
        deregisterMockDriver();
    }

    @AfterAll
    static void finalTeardown() {
        deregisterMockDriver();
    }

    // --- //
    @Test
    void WhenServerNotListening_ThenThrowException() throws SQLException {
        registerMockDriver(
                new MySQLConnectMockDriver(new ServerNotListening(), null, null)
        );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize(1)
                        .connectionCreateTimeout(Duration.ofSeconds(5))
                );

        WarningsAgroalListener warningsListener = new WarningsAgroalListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningsListener) ) {
            var e = assertThrows(RuntimeException.class, () -> dataSource.getConnection());
            assertEquals(CommunicationsException.class, e.getCause().getClass());
        }
        warningsListener.assertAnyFailureStartsWith("java.sql.SQLException: Failed to create connection due to RuntimeException");
    }

    @Test
    void WhenServerAcceptsAndImmediatelyCloseConnection_ThenThrowException() throws SQLException {
        registerMockDriver(
                new MySQLConnectMockDriver(new AcceptConnectionAndClose(), null, null)
        );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize(1)
                        .connectionCreateTimeout(Duration.ofSeconds(1))
                );

        WarningsAgroalListener warningsListener = new WarningsAgroalListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningsListener) ) {
            var e = assertThrows(RuntimeException.class, () -> dataSource.getConnection());
            assertEquals(CommunicationsException.class, e.getCause().getClass());
        }
        warningsListener.assertAnyFailureStartsWith("java.sql.SQLException: Failed to create connection due to RuntimeException");
    }

    @Test
    void WhenServerAcceptsAndDoNotRespond_ThenTimeoutAndTryAgain() throws SQLException {
        registerMockDriver(
                new MySQLConnectMockDriver(new AcceptConnectionAndDoNotRespond(), Duration.ofMillis(10), Duration.ofSeconds(10)),
                new MockDriver.Empty()
        );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize(1)
                        .connectionCreateTimeout(Duration.ofMillis(1000))
                );

        WarningsAgroalListener warningsListener = new WarningsAgroalListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningsListener) ) {
            try ( Connection c = dataSource.getConnection() ) {
                logger.info( "Got connection " + c + " with some URL" );
                warningsListener.assertAnyWarningStartsWith("java.lang.RuntimeException: Canceled connection attempt, because create connection timed out");
            }
        }
    }
}
