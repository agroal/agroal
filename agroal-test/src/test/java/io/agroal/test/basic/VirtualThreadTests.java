// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.pool.util.VirtualThreadUtil;
import io.agroal.test.MockConnection;
import io.agroal.test.MockDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.CONCURRENCY;
import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.abort;

@Tag( FUNCTIONAL )
@Tag( CONCURRENCY )
@EnabledForJreRange( min = JRE.JAVA_21 )
public class VirtualThreadTests {

    private static final Logger logger = getLogger( VirtualThreadTests.class.getName() );

    @BeforeAll
    static void setup() {
        registerMockDriver( new PinningDriver() );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    @Test
    @DisplayName( "Virtual thread carrier pinning during connection creation" )
    void virtualThreadCarrierPinningTest() throws SQLException, InterruptedException {
        ExecutorService executor;
        try {
            executor = VirtualThreadUtil.newVirtualThreadPerTaskExecutor();
        } catch ( UnsupportedOperationException e ) {
            abort( "Multi-release JAR not active (reactor build)" );
            return;
        }

        int POOL_SIZE = 2, CONCURRENCY = 20, TIMEOUT_MS = 2000;
        CountDownLatch latch = new CountDownLatch( CONCURRENCY );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( POOL_SIZE )
                        .acquisitionTimeout( ofMillis( TIMEOUT_MS ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            for ( int i = 0; i < CONCURRENCY; i++ ) {
                executor.submit( () -> {
                    try ( Connection connection = dataSource.getConnection() ) {
                        Thread.sleep( 50 );
                        latch.countDown();
                    } catch ( SQLException e ) {
                        fail( "Unexpected SQLException: " + e.getMessage() );
                    } catch ( InterruptedException e ) {
                        Thread.currentThread().interrupt();
                    }
                } );
            }
            assertTrue( latch.await( TIMEOUT_MS + 1000, MILLISECONDS ), "Virtual threads could not acquire connections within timeout — carrier pinning likely" );
            logger.info( format( "All {0} virtual threads acquired connections successfully", CONCURRENCY ) );
        } finally {
            executor.shutdownNow();
        }
    }

    // --- //

    /**
     * Simulates a JDBC driver that uses synchronized during connection creation,
     * which pins virtual thread carriers.
     */
    public static class PinningDriver extends MockDriver.Empty {

        private static final Object LOCK = new Object();

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            synchronized ( LOCK ) {
                try {
                    Thread.sleep( 200 );
                } catch ( InterruptedException e ) {
                    Thread.currentThread().interrupt();
                }
            }
            return new MockConnection.Empty();
        }
    }
}
