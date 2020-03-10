// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.narayana;

import com.arjuna.ats.arjuna.common.recoveryPropertyManager;
import com.arjuna.ats.arjuna.recovery.RecoveryManager;
import com.arjuna.ats.jta.common.jtaPropertyManager;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.agroal.narayana.NarayanaTransactionIntegration;
import io.agroal.test.MockXAConnection;
import io.agroal.test.MockXADataSource;
import org.jboss.tm.XAResourceRecovery;
import org.jboss.tm.XAResourceRecoveryRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.sql.XAConnection;
import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;
import javax.transaction.xa.XAResource;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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

        DriverAgroalDataSourceListener listener = new DriverAgroalDataSourceListener();
        DriverResourceRecoveryRegistry xaResourceRecoveryRegistry = new DriverResourceRecoveryRegistry( listener );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
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

    @Test
    @DisplayName( "Use supplied recovery specific credentials" )
    public void recoveryCredentials() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();
        RecoveryCredentialsXAResourceRecoveryRegistry xaResourceRecoveryRegistry = new RecoveryCredentialsXAResourceRecoveryRegistry();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry, "", false, xaResourceRecoveryRegistry ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( RecoveryCredentialsXADataSource.class )
                                .principal( new NamePrincipal( RecoveryCredentialsXADataSource.DEFAULT_USER ) )
                                .credential( new SimplePassword( RecoveryCredentialsXADataSource.DEFAULT_PASSWORD ) )
                                .recoveryPrincipal( new NamePrincipal( RecoveryCredentialsXADataSource.RECOVERY_USER ) )
                                .recoveryCredential( new SimplePassword( RecoveryCredentialsXADataSource.RECOVERY_PASSWORD ) ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new WarningsAgroalDatasourceListener() ) ) {
            // Recovery connections are not recorded for pool metrics
            assertEquals( 0, dataSource.getMetrics().creationCount() );
        }
    }

    @Test
    @DisplayName( "Reuse credentials when no recovery specific credentials are supplied" )
    public void reuseCredentials() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();
        RecoveryCredentialsXAResourceRecoveryRegistry xaResourceRecoveryRegistry = new RecoveryCredentialsXAResourceRecoveryRegistry();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry, "", false, xaResourceRecoveryRegistry ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( RecoveryCredentialsXADataSource.class )
                                .principal( new NamePrincipal( RecoveryCredentialsXADataSource.DEFAULT_USER ) )
                                .credential( new SimplePassword( RecoveryCredentialsXADataSource.DEFAULT_PASSWORD ) )
                                .jdbcProperty( "UseDefault", "true" ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new WarningsAgroalDatasourceListener() ) ) {
            logger.info( "Test for reused recovery credentials created connection " + dataSource.getConnection() );
        }
    }

    @Test
    @DisplayName( "Close recovery connection" )
    public void closeRecoveryConnection() throws SQLException, InterruptedException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry, "", false, new NarayanaResourceRecoveryRegistry() ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( RequiresCloseXADataSource.class ) )
                );

        recoveryPropertyManager.getRecoveryEnvironmentBean().setRecoveryBackoffPeriod( 1 );
        recoveryPropertyManager.getRecoveryEnvironmentBean().setPeriodicRecoveryPeriod( 2 );
        CountDownLatch latch = new CountDownLatch( 2 );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new WarningsAgroalDatasourceListener() ) ) {
            logger.info( "Starting recovery on DataSource " + dataSource );
            RecoveryManager.manager().scan( latch::countDown );
            latch.await( 5, TimeUnit.SECONDS );
        }
        assertEquals( 2, RequiresCloseXADataSource.closed, "Recovery connection not closed" );
    }

    // --- //

    private static class DriverAgroalDataSourceListener implements AgroalDataSourceListener {

        private boolean warning = false;

        @Override
        public void onWarning(String message) {
            logger.info( "EXPECTED WARNING: " + message );
            warning = true;
        }

        @Override
        public void onWarning(Throwable throwable) {
            logger.info( "EXPECTED WARNING: " + throwable.getMessage() );
            warning = true;
        }

        public boolean hasWarning() {
            return warning;
        }
    }

    private static class DriverResourceRecoveryRegistry implements XAResourceRecoveryRegistry {

        private final DriverAgroalDataSourceListener listener;
        private final Collection<XAResourceRecovery> xaResourceRecoverySet = new HashSet<>();
        private boolean registered = false;

        public DriverResourceRecoveryRegistry(DriverAgroalDataSourceListener listener) {
            this.listener = listener;
        }

        @Override
        public void addXAResourceRecovery(XAResourceRecovery recovery) {
            assertFalse( listener.hasWarning() );
            assertEquals( 0, recovery.getXAResources().length, "Should not really provide any resources, it's a non-XA ConnectionFactory!!!" );
            assertTrue( listener.hasWarning(), "Should have got a warning for getXAResources on a non-XA ConnectionFactory" );

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

    // --- //

    private static class WarningsAgroalDatasourceListener implements AgroalDataSourceListener {

        @Override
        public void onWarning(String message) {
            fail( "Unexpected warning: " + message );
        }

        @Override
        public void onWarning(Throwable throwable) {
            fail( "Unexpected warning", throwable );
        }
    }

    public static class RecoveryCredentialsXADataSource implements MockXADataSource {

        private static final String DEFAULT_USER = "randomUser";
        private static final String RECOVERY_USER = "recoveryUser";

        private static final String DEFAULT_PASSWORD = "secure";
        private static final String RECOVERY_PASSWORD = "evenMoreSecure";

        private String user;
        private String password;
        private boolean useDefault;

        public void setUser(String user) {
            this.user = user;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public void setUseDefault(boolean reuseDefault) {
            this.useDefault = reuseDefault;
        }

        @Override
        public XAConnection getXAConnection() throws SQLException {
            if ( useDefault ) {
                assertEquals( DEFAULT_USER, user );
                assertEquals( DEFAULT_PASSWORD, password );
            } else {
                assertEquals( RECOVERY_USER, user );
                assertEquals( RECOVERY_PASSWORD, password );
            }
            return new MockXAConnection.Empty();
        }
    }

    private static class RecoveryCredentialsXAResourceRecoveryRegistry implements XAResourceRecoveryRegistry {

        private final Collection<XAResourceRecovery> xaResourceRecoverySet = new HashSet<>();

        @Override
        public void addXAResourceRecovery(XAResourceRecovery recovery) {
            xaResourceRecoverySet.add( recovery );
            recovery.getXAResources();
        }

        @Override
        public void removeXAResourceRecovery(XAResourceRecovery recovery) {
            assertTrue( xaResourceRecoverySet.contains( recovery ), "The recovery to remove is not registered" );
            xaResourceRecoverySet.remove( recovery );
        }
    }

    // --- //

    public static class RequiresCloseXADataSource implements MockXADataSource {

        private static int closed = 0;

        @Override
        public XAConnection getXAConnection() throws SQLException {
            return new MockXAConnection() {
                @Override
                public void close() throws SQLException {
                    logger.info( "Closing XAConnection " + this );
                    closed++;
                }
            };
        }
    }

    private static class NarayanaResourceRecoveryRegistry implements XAResourceRecoveryRegistry {

        @Override
        public void addXAResourceRecovery(XAResourceRecovery recovery) {
            List<com.arjuna.ats.jta.recovery.XAResourceRecovery> recoveries = jtaPropertyManager.getJTAEnvironmentBean().getXaResourceRecoveries();
            recoveries.add( new XAResourceRecoveryAdaptor( recovery ) );
            jtaPropertyManager.getJTAEnvironmentBean().setXaResourceRecoveries( recoveries );
        }

        @Override
        public void removeXAResourceRecovery(XAResourceRecovery recovery) {
            logger.info( "Removing recovery " + recovery );
            List<com.arjuna.ats.jta.recovery.XAResourceRecovery> recoveries = jtaPropertyManager.getJTAEnvironmentBean().getXaResourceRecoveries();
            recoveries.remove( new XAResourceRecoveryAdaptor( recovery ) );
            jtaPropertyManager.getJTAEnvironmentBean().setXaResourceRecoveries( recoveries );
            assertTrue( recoveries.isEmpty(), "Expected empty recoveries" );
        }
    }

    private static class XAResourceRecoveryAdaptor implements com.arjuna.ats.jta.recovery.XAResourceRecovery {
        private final XAResourceRecovery xaResourceRecovery;
        private int count = 0;

        public XAResourceRecoveryAdaptor(XAResourceRecovery xaResourceRecovery) {
            this.xaResourceRecovery = xaResourceRecovery;
        }

        @Override
        public XAResource getXAResource() throws SQLException {
            XAResource resource = xaResourceRecovery.getXAResources()[count++];
            logger.info( "Retrieving XAResource " + resource );
            return resource;
        }

        @Override
        public boolean initialise(String p) throws SQLException {
            return true;
        }

        @Override
        public boolean hasMoreResources() {
            if ( count > 0 ) {
                count = 0;
                return false;
            }
            return true;
        }

        @Override
        public boolean equals(Object o) {
            if ( this == o ) {
                return true;
            } else if ( o == null || getClass() != o.getClass() ) {
                return false;
            }
            XAResourceRecoveryAdaptor that = (XAResourceRecoveryAdaptor) o;
            return xaResourceRecovery.equals( that.xaResourceRecovery );
        }

        @Override
        public int hashCode() {
            return Objects.hash( xaResourceRecovery );
        }
    }
}
