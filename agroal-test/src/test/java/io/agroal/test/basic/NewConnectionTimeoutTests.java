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
import static org.junit.jupiter.api.Assertions.fail;

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
                fail("Not supposed to create a connection");
            }catch (SQLException e){
                // Connection creation was started
                connectionListener.assertConnectionCreationStarted();

                // But Acquisition timed out
                assertTrue(e.getMessage().contains("Acquisition"));

                // ConnectionCreation was canceled
                connectionListener.assertConnectionCanceled();

                // even if we give some time to interrupt, cancel will not take effect in the connection creation thread...
                Thread.sleep(5000);

                // Single thread for creating the db connection is still running and hangs - can´t be canceled
                // Which will block the pool, and new connections couldn´t be created
                connectionListener.assertNoConnectionCreated();
                warningsListener.assertNoConnectionFailures();
            }

            warningsListener.reset();
            connectionListener.reset();

            // Proof that NO new db connection can be created
            try ( Connection c = dataSource.getConnection() ) {
                // Supposed to fail
                fail("Not supposed to create a connection");
            }catch (SQLException e){

                // Single priority thread in PriorityScheduledExecutor is still blocked with previous connection creation
                // therefore connection creation was NOT started
                connectionListener.assertNoConnectionCreationStarted();

                // Second attempt also timed out
                assertTrue(e.getMessage().contains("Acquisition"));

                // Single thread for creating the db connection is still running and hangs - can´t be canceled
                // Which will block the pool, and new connections couldn´t be created
                connectionListener.assertNoConnectionCreated();
                warningsListener.assertNoConnectionFailures();
            }
        }
    }

    @Test
    void WhenServerAcceptsAndDoNotRespond_ThenNextConnectionCreationAttemptWillBeSuccessful() throws SQLException, InterruptedException {
        registerMockDriver(
                new MySQLConnectMockDriver(new AcceptConnectionAndDoNotRespond()),
                new MockDriver.Empty()
        );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize(1)
                        .acquisitionTimeout(Duration.ofSeconds(1))
                        .connectionCreateTimeout(Duration.ofSeconds(2))
                );

        WarningsAgroalListener warningsListener = new WarningsAgroalListener();
        ConnectionListener connectionListener = new ConnectionListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningsListener, connectionListener) ) {
            try ( Connection c = dataSource.getConnection() ) {
                // Supposed to fail
                fail("Not supposed to create a connection");
            }catch (SQLException e){
                // Connection creation was started
                connectionListener.assertConnectionCreationStarted();

                // Acquisition timed out
                assertTrue(e.getMessage().contains("Acquisition"));

                // ConnectionCreation was not attempted to cancel because
                // we do not cancel if there is a dedicated connectionCreateTimeout
                connectionListener.assertNoConnectionCanceled();

                // wait for ConnectionCreation time out...
                Thread.sleep(1500);

                // ConnectionCreation canceled due to connectionCreateTimeout
                connectionListener.assertConnectionCanceled();

                // Thread for creating the db connection is still running and hangs
                // But that does not block anymore the pool
                connectionListener.assertNoConnectionCreated();
                warningsListener.assertNoConnectionFailures();
            }

            warningsListener.reset();
            connectionListener.reset();

            // Proof that new db connection can be created, even if the first connection creation attempt still running and hanging!
            try ( Connection c = dataSource.getConnection() ) {
                logger.info( "Got connection " + c + " with some URL" );
                connectionListener.assertConnectionCreationStarted();
                connectionListener.assertConnectionCreated();
            }
        }
    }
}
