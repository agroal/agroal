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
import io.agroal.test.fakeserver.NotAcceptConnection;
import io.agroal.test.fakeserver.ServerNotListening;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag( FUNCTIONAL )
public class NewConnectionTimeoutTests {

    static final Logger logger = getLogger( NewConnectionTimeoutTests.class.getName() );

    @BeforeAll
    static void setup() {
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
    void WhenServerNotListening_ThenThrowExceptionAndTryAgain() throws SQLException {
        registerMockDriver( new MySQLConnectMockDriver( new ServerNotListening() ), new MockDriver.Empty() );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp.maxSize( 1 ) );

        WarningsAgroalListener warningsListener = new WarningsAgroalListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningsListener ) ) {
            var e = assertThrows( RuntimeException.class, dataSource::getConnection );
            assertEquals( CommunicationsException.class, e.getCause().getClass() );
            warningsListener.assertAnyFailureStartsWith( "java.sql.SQLException: Failed to create connection due to RuntimeException" );
            warningsListener.reset();

            // subsequent call to MockDriver.Empty succeeds
            try ( Connection c = dataSource.getConnection() ) {
                logger.info( "Got connection " + c + " with some URL" );
                warningsListener.assertNoConnectionFailures();
            }
        }
    }

    @Test
    void WhenServerNotAcceptConnection_ThenThrowExceptionAndTryAgain() throws SQLException {
        registerMockDriver( new MySQLConnectMockDriver( new NotAcceptConnection() ), new MockDriver.Empty() );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .connectionFactoryConfiguration( cf -> cf
                                .loginTimeout( Duration.ofSeconds( 1 ) ) // .jdbcProperty( "socketTimeout", "1000" )
                        )
                );

        WarningsAgroalListener warningsListener = new WarningsAgroalListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningsListener ) ) {
            var e = assertThrows( RuntimeException.class, dataSource::getConnection );
            assertEquals( CommunicationsException.class, e.getCause().getClass() );
            warningsListener.assertAnyFailureStartsWith( "java.sql.SQLException: Failed to create connection due to RuntimeException" );
            warningsListener.reset();

            // subsequent call to MockDriver.Empty succeeds
            try ( Connection c = dataSource.getConnection() ) {
                logger.info( "Got connection " + c + " with some URL" );
                warningsListener.assertNoConnectionFailures();
            }
        }
    }

    @Test
    void WhenServerAcceptsAndImmediatelyCloseConnection_ThenThrowExceptionAndTryAgain() throws SQLException {
        registerMockDriver( new MySQLConnectMockDriver( new AcceptConnectionAndClose() ), new MockDriver.Empty() );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp.maxSize( 1 ) );

        WarningsAgroalListener warningsListener = new WarningsAgroalListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningsListener ) ) {
            var e = assertThrows( RuntimeException.class, dataSource::getConnection );
            assertEquals( CommunicationsException.class, e.getCause().getClass() );
            warningsListener.assertAnyFailureStartsWith( "java.sql.SQLException: Failed to create connection due to RuntimeException" );
            warningsListener.reset();

            // subsequent call to MockDriver.Empty succeeds
            try ( Connection c = dataSource.getConnection() ) {
                logger.info( "Got connection " + c + " with some URL" );
                warningsListener.assertNoConnectionFailures();
            }
        }
    }

    @Test
    void WhenServerAcceptsAndDoNotRespond_ThenThrowExceptionAndTryAgain() throws SQLException {
        registerMockDriver( new MySQLConnectMockDriver( new AcceptConnectionAndDoNotRespond() ), new MockDriver.Empty() );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .connectionFactoryConfiguration( cf -> cf
                                .loginTimeout( Duration.ofSeconds( 1 ) ) // .jdbcProperty( "socketTimeout", "1000" )
                        )
                );

        WarningsAgroalListener warningsListener = new WarningsAgroalListener();
        ConnectionListener connectionListener = new ConnectionListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningsListener, connectionListener ) ) {

            // call to MySQLConnectMockDriver fails

            var e = assertThrows( RuntimeException.class, dataSource::getConnection );
            assertEquals( CommunicationsException.class, e.getCause().getClass() );
            assertTrue( e.getMessage().contains( "Communications link failure" ) );

            connectionListener.assertConnectionCreationStarted();
            connectionListener.assertNoConnectionCreated();
            warningsListener.assertAnyFailureStartsWith( "java.sql.SQLException: Failed to create connection due to RuntimeException" );

            warningsListener.reset();
            connectionListener.reset();

            // subsequent call to MockDriver.Empty succeeds
            try ( Connection c = dataSource.getConnection() ) {
                logger.info( "Got connection " + c + " with some URL" );
                assertEquals( 1, connectionListener.getStartedConnectionCreations() );

                connectionListener.assertConnectionCreated();
                warningsListener.assertNoConnectionFailures();
            }
        }
    }

    @Test
    void WhenServerAcceptsAndDoNotRespond_ThenAcquisitionTimeoutAndConnectionCreationHangs() throws Exception {
        registerMockDriver( new MySQLConnectMockDriver( new AcceptConnectionAndDoNotRespond() ), new MockDriver.Empty() );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .acquisitionTimeout( Duration.ofSeconds( 1 ) )
                );

        WarningsAgroalListener warningsListener = new WarningsAgroalListener();
        ConnectionListener connectionListener = new ConnectionListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningsListener, connectionListener ) ) {
            try {
                assertTimeoutPreemptively( Duration.ofSeconds( 2 ), () -> assertThrows( SQLException.class, dataSource::getConnection ), "Expecting getConnection to hang" );
                fail( "Not supposed to create a connection" ); // Supposed to fail
            } catch ( Error e ) {
                // Connection creation was started but Acquisition timed out
                connectionListener.assertConnectionCreationStarted();
                assertTrue( e.getMessage().contains( "execution timed out after" ) );

                // even if we give some time to interrupt, cancel will not take effect in the connection creation thread...
                Thread.sleep( 2000 );

                // Single thread for creating the db connection is still running and hangs - can't be canceled
                // Which will block the pool, and new connections couldn't be created
                connectionListener.assertNoConnectionCreated();
                warningsListener.assertNoConnectionFailures();
            }

            warningsListener.reset();
            connectionListener.reset();

            // Proof that NO new db connection can be created
            try {
                assertTimeoutPreemptively( Duration.ofSeconds( 2 ), () -> assertThrows( SQLException.class, dataSource::getConnection ), "Expecting getConnection to hang" );
                fail( "Not supposed to create a connection" ); // Supposed to fail
            } catch ( Error e ) {
                // Thread is still blocked with previous connection creation therefore connection creation was NOT started
                connectionListener.assertNoConnectionCreationStarted();

                // Second attempt also timed out
                assertTrue( e.getMessage().contains( "execution timed out after" ) );

                // Single thread for creating the db connection is still running and hangs - can't be canceled
                // Which will block the pool, and new connections couldn't be created
                connectionListener.assertNoConnectionCreated();
                warningsListener.assertNoConnectionFailures();
            }
        }
    }

    @Test
    void WhenServerAcceptsAndDoNotRespond_ThenNextConnectionCreationAttemptWillBeUnsuccessful() throws Exception {
        registerMockDriver( new MySQLConnectMockDriver( new AcceptConnectionAndDoNotRespond() ), new SlowDriver( Duration.ofMillis( 500 ) ) );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .acquisitionTimeout( Duration.ofMillis( 1500 ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .loginTimeout( Duration.ofSeconds( 1 ) )
                        )
                );

        WarningsAgroalListener warningsListener = new WarningsAgroalListener();
        ConnectionListener connectionListener = new ConnectionListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningsListener, connectionListener ) ) {

            // call to MySQLConnectMockDriver fails and call to SlowDriver does not finish in time

            var e = assertThrows( RuntimeException.class, dataSource::getConnection );
            assertEquals( CommunicationsException.class, e.getCause().getClass() ); // the exception here is the one of the first attempt
            assertTrue( e.getMessage().contains( "Communications link failure" ) );

            assertEquals( 1, connectionListener.getStartedConnectionCreations() );
            connectionListener.assertNoConnectionCreated();
            warningsListener.assertAnyFailureStartsWith( "java.sql.SQLException: Failed to create connection due to RuntimeException" );

            try ( Connection c = dataSource.getConnection() ) {
                // AG-290 - because the thread that is waiting for the slow driver to return has to remain blocked anyway
                // for longer than acquisitionTimeout
                // when that call returns successfully there is no need to throw an exception. the call completes successfully.
                connectionListener.assertConnectionCreated();
                assertEquals( 2, connectionListener.getStartedConnectionCreations() );
            }
        }
    }

    @Test
    void WhenServerAcceptsAndDoNotRespond_ThenNextConnectionCreationAttemptWillBeSuccessful() throws Exception {
        registerMockDriver( new MySQLConnectMockDriver( new AcceptConnectionAndDoNotRespond() ), new MockDriver.Empty() );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .acquisitionTimeout( Duration.ofSeconds( 3 ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .loginTimeout( Duration.ofSeconds( 1 ) )
                        )
                );

        WarningsAgroalListener warningsListener = new WarningsAgroalListener();
        ConnectionListener connectionListener = new ConnectionListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningsListener, connectionListener ) ) {
            warningsListener.assertNoConnectionFailures();

            try ( Connection c = dataSource.getConnection() ) {
                fail( "getConnection() succeeded unexpectedly!" );
            } catch ( RuntimeException e ) {
                logger.info( "Got expected runtime exception with message: " + e.getMessage() );
                assertEquals( CommunicationsException.class, e.getCause().getClass() );
                assertTrue( e.getMessage().contains( "Communications link failure" ) );
            }

            try ( Connection c = dataSource.getConnection() ) {
                logger.info( "Got connection " + c + " with some URL" );

                // Connection creation was started but the first failed
                assertEquals( 2, connectionListener.getStartedConnectionCreations() );
                assertEquals( 1, warningsListener.failuresCount() );

                // the retry succeeded
                connectionListener.assertConnectionCreated();
            }
        }
    }

    // --- //

    private static class SlowDriver extends MockDriver.Empty {

        private final Duration wait;

        public SlowDriver(Duration d) {
            wait = d;
        }

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            try {
                Thread.sleep( wait.toMillis() );
                return super.connect( url, info );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( e );
            }
        }
    }
}
