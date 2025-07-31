// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.MockDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import static io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL_POOLLESS;
import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.lang.Thread.currentThread;
import static java.text.MessageFormat.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
class PoollessTests {

    static final Logger logger = getLogger( PoollessTests.class.getName() );

    @BeforeAll
    static void setupMockDriver() {
        registerMockDriver();
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Pool-less Test" )
    @SuppressWarnings( {"AnonymousInnerClassMayBeStatic", "ObjectAllocationInLoop", "JDBCResourceOpenedButNotSafelyClosed"} )
    void poollessTest() throws SQLException {
        int TIMEOUT_MS = 100, NUM_THREADS = 5;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .dataSourceImplementation( AGROAL_POOLLESS )
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( 1 ) // ignored
                        .minSize( 1 ) // ignored
                        .maxSize( 2 )
                        .acquisitionTimeout( Duration.ofMillis( 6 * TIMEOUT_MS ) )
                );

        CountDownLatch destroyLatch = new CountDownLatch( 1 );

        AgroalDataSourceListener listener = new AgroalDataSourceListener() {
            @Override
            public void onConnectionDestroy(Connection connection) {
                destroyLatch.countDown();
            }

            @Override
            public void onWarning(String message) {
                logger.info( message );
            }

            @Override
            public void onWarning(Throwable throwable) {
                fail( throwable );
            }
        };

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            assertEquals( 0, dataSource.getMetrics().creationCount() );

            try ( Connection c = dataSource.getConnection() ) {
                assertFalse( c.isClosed() );

                try ( Connection testSubject = dataSource.getConnection() ) {
                    assertFalse( testSubject.isClosed() );

                    assertEquals( 2, dataSource.getMetrics().creationCount() );
                    assertThrows( SQLException.class, dataSource::getConnection, "Expected exception due to pool being full" );
                    assertEquals( 2, dataSource.getMetrics().creationCount() );
                }

                logger.info( format( "Waiting for destruction of connection" ) );
                if ( !destroyLatch.await( 2 * TIMEOUT_MS, MILLISECONDS ) ) {
                    fail( format( "Flushed connections not sent for destruction" ) );
                }

                // One connection flushed and another in use
                assertEquals( 1, dataSource.getMetrics().flushCount() );
                assertEquals( 1, dataSource.getMetrics().activeCount() );
                assertEquals( 1, dataSource.getMetrics().availableCount() );
            }

            // Assert min-size is zero
            assertEquals( 0, dataSource.getMetrics().activeCount() );
            assertEquals( 2, dataSource.getMetrics().availableCount() );
            assertEquals( 2, dataSource.getMetrics().flushCount() );

            // Assert that closing a connection unblocks one waiting thread
            assertDoesNotThrow( () -> {
                Connection c = dataSource.getConnection();
                dataSource.getConnection();

                // just one of this threads will unblock
                Collection<Thread> threads = new ArrayList<>( NUM_THREADS );
                for ( int i = 0; i < NUM_THREADS; i++ ) {
                    threads.add( newConnectionThread( dataSource ) );
                }
                threads.forEach( Thread::start );

                try {
                    Thread.sleep( TIMEOUT_MS );
                    assertEquals( NUM_THREADS, dataSource.getMetrics().awaitingCount(), "Insufficient number of blocked threads" );
                    assertEquals( 4, dataSource.getMetrics().creationCount() );

                    logger.info( "Closing connection to unblock one waiting thread" );
                    c.close();

                    Thread.sleep( TIMEOUT_MS );

                    assertEquals( NUM_THREADS - 1, dataSource.getMetrics().awaitingCount(), "Insufficient number of blocked threads" );
                    assertEquals( 5, dataSource.getMetrics().creationCount() );

                    for ( Thread thread : threads ) {
                        thread.join( TIMEOUT_MS );
                    }
                } catch ( InterruptedException e ) {
                    fail( e );
                }
            } );
        } catch ( InterruptedException e ) {
            fail( "Test fail due to interrupt" );
        }

        try {
            Thread.sleep( TIMEOUT_MS );
        } catch ( InterruptedException e ) {
            //
        }
    }

    private static Thread newConnectionThread(DataSource dataSource) {
        return new Thread( () -> {
            logger.info( currentThread().getName() + " is on the race for a connection" );
            try {
                Connection c = dataSource.getConnection();
                assertFalse( c.isClosed() );
                logger.info( currentThread().getName() + " got one connection !!!" );
            } catch ( CancellationException | SQLException e ) {
                logger.info( currentThread().getName() + " got none" );
            }
        } );
    }

    // --- //

    @Test
    @DisplayName( "Exception on create connection" )
    void createExceptionTest() throws SQLException {
        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .dataSourceImplementation( AGROAL_POOLLESS )
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( ExceptionalDataSource.class )
                        ) );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            try ( Connection c = dataSource.getConnection() ) {
                fail( "Got connection " + c );
            } catch ( SQLException e ) {
                // test for AG-194 --- active count was incremented incorrectly
                assertEquals( 0, dataSource.getMetrics().activeCount(), "Active count incremented after exception" );
            }
        }
    }

    public static class ExceptionalDataSource extends MockDataSource.Empty {

        @Override
        public Connection getConnection() throws SQLException {
            throw new SQLException( "Exceptional condition" );
        }
    }
}
