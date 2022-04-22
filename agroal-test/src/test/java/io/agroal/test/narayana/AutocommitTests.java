// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.narayana;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.narayana.NarayanaTransactionIntegration;
import io.agroal.test.MockConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
@Tag( TRANSACTION )
public class AutocommitTests {

    private static final Logger logger = getLogger( AutocommitTests.class.getName() );

    @BeforeAll
    static void setup() {
        registerMockDriver( AutoCommitTrackerConnection.class );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "AutoCommit reset test" )
    void testAutoCommitReset() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                        .connectionFactoryConfiguration( cf -> cf.autoCommit( false ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            txManager.begin();

            // Since auto-commit is set to false in the configuration, no further calls to setAutocommit are expected from Agroal

            Connection connection = dataSource.getConnection();
            logger.info( format( "Got connection {0}", connection ) );

            txManager.commit();
        } catch ( NotSupportedException | SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e ) {
            fail( "Exception: " + e.getMessage() );
        }
    }

    // --- //

    public static class AutoCommitTrackerConnection implements MockConnection {

        private boolean autoCommit = true;

        private int count;

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            this.autoCommit = autoCommit;
            if ( count++ > 0 ) {
                // auto-commit is on by default, so the first call to set it to false is allowed
                fail();
            }
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return autoCommit;
        }
    }
}
