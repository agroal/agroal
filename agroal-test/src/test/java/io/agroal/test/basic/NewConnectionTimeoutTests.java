// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.ConnectionListener;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                new MySQLConnectMockDriver(new ServerNotListening())
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
    void WhenServerAcceptsAndImmediatelyCloseConnection_ThenThrowException() throws SQLException {
        registerMockDriver(
                new MySQLConnectMockDriver(new AcceptConnectionAndClose())
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
    void WhenServerAcceptsAndDoNotRespond_ThenConnectCreateTimeoutAndTryAgain() throws SQLException {
        registerMockDriver(
                new MySQLConnectMockDriver(new AcceptConnectionAndDoNotRespond()),
                new MockDriver.Empty()
        );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize(1)
                        .connectionCreateTimeout(Duration.ofSeconds(1))
                );

        WarningsAgroalListener warningsListener = new WarningsAgroalListener();
        ConnectionListener connectionListener = new ConnectionListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningsListener, connectionListener) ) {
            try ( Connection c = dataSource.getConnection() ) {
                logger.info( "Got connection " + c + " with some URL" );
                assertEquals(2, connectionListener.getStartedConnectionCreations());
                assertEquals(1, connectionListener.getCanceledConnections());
                connectionListener.assertConnectionCreated();
                warningsListener.assertAnyWarningStartsWith("java.lang.RuntimeException: Canceled connection attempt, because create connection timed out");
            }
        }
    }

    @Test
    void WhenServerAcceptsAndDoNotRespond_ThenAcquisitionTimeoutAndConnectionCreationHangs() throws SQLException, InterruptedException {
        registerMockDriver(
                new MySQLConnectMockDriver(new AcceptConnectionAndDoNotRespond()),
                new MockDriver.Empty()
        );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize(1)
                        .acquisitionTimeout(Duration.ofSeconds(1))
                );

        WarningsAgroalListener warningsListener = new WarningsAgroalListener();
        ConnectionListener connectionListener = new ConnectionListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningsListener, connectionListener) ) {
            try ( Connection c = dataSource.getConnection() ) {
                // Supposed to fail
            }catch (SQLException e){
                // Connection creation was started
                connectionListener.assertConnectionCreationStarted();

                // But Acquisition timed out
                assertTrue(e.getMessage().contains("Acquisition"));

                // ConnectionCreation is canceled
                connectionListener.assertConnectionCanceled();

                // give the interrupt some time...
                Thread.sleep(5000);

                // Single thread for creating the db connection is still running and hangs
                // Which will block the pool, and new connections couldn´t be created
                connectionListener.assertNoConnectionCreated();
                warningsListener.assertNoConnectionFailures();
            }
        }
    }
}
