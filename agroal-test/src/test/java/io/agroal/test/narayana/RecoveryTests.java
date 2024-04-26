// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.narayana;

import com.arjuna.ats.arjuna.recovery.RecoveryManager;
import com.arjuna.ats.internal.jta.recovery.arjunacore.XARecoveryModule;
import com.arjuna.ats.jbossatx.jta.RecoveryManagerService;
import com.arjuna.ats.jta.recovery.XAResourceRecoveryHelper;
import com.arjuna.ats.jta.xa.XidImple;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.agroal.narayana.NarayanaTransactionIntegration;
import io.agroal.test.MockXAConnection;
import io.agroal.test.MockXADataSource;
import io.agroal.test.MockXAResource;
import org.jboss.tm.XAResourceRecovery;
import org.jboss.tm.XAResourceRecoveryRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import jakarta.transaction.TransactionManager;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;

import static com.arjuna.ats.arjuna.recovery.RecoveryManager.DIRECT_MANAGEMENT;
import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.util.logging.Logger.getLogger;
import static javax.transaction.xa.XAResource.TMENDRSCAN;
import static javax.transaction.xa.XAResource.TMSTARTRSCAN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
@Tag( TRANSACTION )
public class RecoveryTests {

    static final Logger logger = getLogger( RecoveryTests.class.getName() );

    @BeforeAll
    static void setup() {
        registerMockDriver();
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @SuppressWarnings( "ConstantConditions" )
    @DisplayName( "Register ConnectionFactory into XAResourceRecoveryRegistry" )
    void registerXAResourceRecoveryTest() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        DriverResourceRecoveryRegistry xaResourceRecoveryRegistry = new DriverResourceRecoveryRegistry();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry, "", false, xaResourceRecoveryRegistry ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .autoCommit( true ) )
                );

        assertFalse( xaResourceRecoveryRegistry.isRegistered(), "ConnectionFactory prematurely registered in XAResourceRecoveryRegistry" );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, xaResourceRecoveryRegistry.getListener() ) ) {
            logger.info( "Test for recovery registration created datasource " + dataSource );

            assertTrue( xaResourceRecoveryRegistry.isRegistered(), "ConnectionFactory not registered in XAResourceRecoveryRegistry" );
        }

        assertFalse( xaResourceRecoveryRegistry.isRegistered(), "ConnectionFactory not de-registered in XAResourceRecoveryRegistry" );
    }

    @Test
    @DisplayName( "Use supplied recovery specific credentials" )
    void recoveryCredentials() throws SQLException {
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

    @SuppressWarnings( "JDBCResourceOpenedButNotSafelyClosed" )
    @Test
    @DisplayName( "Reuse credentials when no recovery specific credentials are supplied" )
    void reuseCredentials() throws SQLException {
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
    void closeRecoveryConnection() throws Exception {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        com.arjuna.ats.arjuna.common.recoveryPropertyManager.getRecoveryEnvironmentBean().setRecoveryBackoffPeriod( 1 );
        RecoveryManager recoveryManager = RecoveryManager.manager( DIRECT_MANAGEMENT );

        RecoveryManagerService recoveryService = new RecoveryManagerService();
        recoveryService.create();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry, "", false, recoveryService ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( RequiresCloseXADataSource.class ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new WarningsAgroalDatasourceListener() ) ) {
            logger.info( "Starting recovery on DataSource " + dataSource );
            recoveryManager.scan();
            logger.info( "Performed first scan. Performing a second scan" );
            recoveryManager.scan();
            logger.info( "Two recovery scans completed" );
        }
        assertEquals( 2, RequiresCloseXADataSource.getClosed(), "Recovery connection not closed" );
    }

    @Test
    @DisplayName( "XAResource reopen test" )
    void xaResourceReopenTest() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        com.arjuna.ats.arjuna.common.recoveryPropertyManager.getRecoveryEnvironmentBean().setRecoveryBackoffPeriod( 1 );
        RecoveryManager recoveryManager = RecoveryManager.manager( DIRECT_MANAGEMENT );
        recoveryManager.removeAllModules( true );
        recoveryManager.addModule( new DoublePassXARecoveryModule() );

        RecoveryManagerService recoveryService = new RecoveryManagerService();
        recoveryService.create();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry, "", false, recoveryService ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( RequiresCloseXADataSource.class ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new WarningsAgroalDatasourceListener() ) ) {
            logger.info( "Starting recovery on DataSource " + dataSource );
            recoveryManager.scan();

            assertEquals( 2, RequiresCloseXADataSource.getClosed(), "Expected two connections to have been closed" );
        } finally {
            recoveryManager.terminate( true );
        }
    }

    @Test
    @DisplayName( "Check all connections are closed test" )
    void xaConnectionsAreClosedTest() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        com.arjuna.ats.arjuna.common.recoveryPropertyManager.getRecoveryEnvironmentBean().setRecoveryBackoffPeriod( 1 );
        RecoveryManager recoveryManager = RecoveryManager.manager( DIRECT_MANAGEMENT );
        recoveryManager.removeAllModules( true );
        recoveryManager.addModule( new XARecoveryModule() );

        RecoveryManagerService recoveryService = new RecoveryManagerService();
        recoveryService.create();

        AgroalDataSourceConfigurationSupplier configurationSupplier1 = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry, "", false, recoveryService ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( RequiresCloseXADataSource.class ) )
                );
        AgroalDataSourceConfigurationSupplier configurationSupplier2 = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry, "", false, recoveryService ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( RequiresCloseXADataSource.class ) )
                );
        try ( AgroalDataSource dataSource1 = AgroalDataSource.from( configurationSupplier1, new WarningsAgroalDatasourceListener() );
        		AgroalDataSource dataSource2 = AgroalDataSource.from( configurationSupplier2, new WarningsAgroalDatasourceListener()) ) {
            logger.info( "Starting recovery on DataSources " +  dataSource1 + "and " + dataSource2 );
            
            recoveryManager.scan();

            assertEquals( 2, RequiresCloseXADataSource.getClosed(), "Expected 2 connections to have been closed" );
        } finally {
            recoveryManager.terminate( true );
        }
    }
    // --- //

    private static class DriverAgroalDataSourceListener implements AgroalDataSourceListener {

        private boolean warning;

        DriverAgroalDataSourceListener() {
        }

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

        boolean hasWarning() {
            return warning;
        }
    }

    @SuppressWarnings( "WeakerAccess" )
    private static class DriverResourceRecoveryRegistry implements XAResourceRecoveryRegistry {

        private final DriverAgroalDataSourceListener listener = new DriverAgroalDataSourceListener();
        private final Collection<XAResourceRecovery> xaResourceRecoverySet = new HashSet<>();
        private boolean registered;

        DriverResourceRecoveryRegistry() {
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

        boolean isRegistered() {
            return registered;
        }

        DriverAgroalDataSourceListener getListener() {
            return listener;
        }
    }

    // --- //

    private static class WarningsAgroalDatasourceListener implements AgroalDataSourceListener {

        @SuppressWarnings( "WeakerAccess" )
        WarningsAgroalDatasourceListener() {
        }

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
            useDefault = reuseDefault;
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

        @SuppressWarnings( "WeakerAccess" )
        RecoveryCredentialsXAResourceRecoveryRegistry() {
        }

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

        private static int closed;

        static void incrementClosed() {
            closed++;
        }

        @SuppressWarnings( "WeakerAccess" )
        static int getClosed() {
            return closed;
        }

        public RequiresCloseXADataSource() {
            closed = 0; // reset whenever a new instance is created
        }

        @Override
        public XAConnection getXAConnection() throws SQLException {
            logger.info( "Creating new XAConnection");
            return new MyMockXAConnection();
        }

        private static class MyMockXAConnection implements MockXAConnection {
            MyMockXAConnection() {
            }

            @Override
            public XAResource getXAResource() throws SQLException {
                return new NonEmptyXidsResource();
            }

            @Override
            @SuppressWarnings( "ObjectToString" )
            public void close() throws SQLException {
                logger.info( "Closing XAConnection " + this );
                incrementClosed();
            }
        }
    }

    private static class NonEmptyXidsResource implements MockXAResource {
        @Override
        public Xid[] recover(int flags) throws XAException {
            return flags == TMENDRSCAN ? null : new Xid[] { new XidImple() };
        }
    }

    // --- //

    private static class DoublePassXARecoveryModule extends XARecoveryModule {

        private final Collection<XAResource> xaResources = new ArrayList<>();
        private final Collection<XAResourceRecoveryHelper> xaResourceRecoveryHelpers = new ArrayList<>();


        public void addXAResourceRecoveryHelper(XAResourceRecoveryHelper xaResourceRecoveryHelper) {
            xaResourceRecoveryHelpers.add(xaResourceRecoveryHelper);
        }

        public synchronized void removeXAResourceRecoveryHelper(XAResourceRecoveryHelper xaResourceRecoveryHelper) {
            xaResourceRecoveryHelpers.remove( xaResourceRecoveryHelper );
        }

        @Override
        public synchronized void periodicWorkFirstPass() {
            for ( XAResourceRecoveryHelper helper : xaResourceRecoveryHelpers ) {
                try {
                    xaResources.addAll( List.of( helper.getXAResources() ) );
                } catch ( Exception e ) {
                    fail( "Failure while obtaining XA Resource", e );
                }
            }
            doPeriodicPass();
        }

        @Override
        public synchronized void periodicWorkSecondPass() {
            doPeriodicPass();
            xaResources.clear();
        }

        /*
         * Do a scan for each pass to verify that a new scan can execute after a previous one has completed
         */
        private void doPeriodicPass() {
            for ( XAResource xaResource : xaResources ) {
                try {
                    Xid[] recoveryXids = xaResource.recover( TMSTARTRSCAN );
                    assertNotNull( recoveryXids, "Expecting some Xids" );

                    recoveryXids = xaResource.recover( TMENDRSCAN );
                    assertNull( recoveryXids, "Expecting empty Xids" );
                } catch ( XAException e ) {
                    fail( "Failure while using XA Resource", e );
                }
            }
        }
    }
}
