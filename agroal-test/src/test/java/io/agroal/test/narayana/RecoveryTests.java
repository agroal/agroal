// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.narayana;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.narayana.NarayanaTransactionIntegration;
import org.jboss.tm.XAResourceRecovery;
import org.jboss.tm.XAResourceRecoveryRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
@Tag( TRANSACTION )
public class RecoveryTests {

    private static final Logger logger = getLogger( RecoveryTests.class.getName() );

    @BeforeAll
    public static void setup() {
        registerMockDriver();
    }

    @AfterAll
    public static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Register ConnectionFactory into XAResourceRecoveryRegistry" )
    public void registerXAResourceRecoveryTest() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        boolean[] warning = {false};
        AgroalDataSourceListener listener = new AgroalDataSourceListener() {
            @Override
            public void onWarning(String message) {
                logger.info( message );
                warning[0] = true;
            }
        };

        TestXAResourceRecoveryRegistry xaResourceRecoveryRegistry = new TestXAResourceRecoveryRegistry( warning );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry, "", false, xaResourceRecoveryRegistry ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .autoCommit( true ) )
                );

        assertFalse( xaResourceRecoveryRegistry.isRegistered(), "ConnectionFactory prematurely registered in XAResourceRecoveryRegistry" );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            logger.info( "Test for recovery registration created datasource " + dataSource );

            assertTrue( xaResourceRecoveryRegistry.isRegistered(), "ConnectionFactory not registered in XAResourceRecoveryRegistry" );
        }

        assertFalse( xaResourceRecoveryRegistry.isRegistered(), "ConnectionFactory not de-registered in XAResourceRecoveryRegistry" );
    }

    // --- //

    private static class TestXAResourceRecoveryRegistry implements XAResourceRecoveryRegistry {

        private boolean registered = false;

        private boolean warningFlag[];

        private Collection<XAResourceRecovery> xaResourceRecoverySet = new HashSet<>();

        public TestXAResourceRecoveryRegistry(boolean[] warningFlag) {
            this.warningFlag = warningFlag;
        }

        @Override
        public void addXAResourceRecovery(XAResourceRecovery recovery) {
            assertFalse( warningFlag[0] );
            assertEquals( 0, recovery.getXAResources().length, "Should not really provide any resources, it's a non-XA ConnectionFactory!!!" );
            assertTrue( warningFlag[0], "Should have got a warning for getXAResources on a non-XA ConnectionFactory" );

            xaResourceRecoverySet.add( recovery );
            registered = true;
        }

        @Override
        public void removeXAResourceRecovery(XAResourceRecovery recovery) {
            assertTrue( xaResourceRecoverySet.contains( recovery ), "The recovery to remove is not registered" );

            xaResourceRecoverySet.remove( recovery );
            registered = false;
        }

        public boolean isRegistered() {
            return registered;
        }
    }
}
