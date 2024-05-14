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
import io.agroal.test.basic.Utils;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.TransactionSynchronizationRegistry;
import org.jboss.tm.XAResourceRecovery;
import org.jboss.tm.XAResourceRecoveryRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.arjuna.ats.arjuna.recovery.RecoveryManager.DIRECT_MANAGEMENT;
import static io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL;
import static io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL_POOLLESS;
import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Logger.getLogger;
import static java.util.stream.Collectors.toList;
import static javax.transaction.xa.XAResource.TMENDRSCAN;
import static javax.transaction.xa.XAResource.TMSTARTRSCAN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

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
        if ( Utils.isWindowsOS() ) {
            Utils.windowsTimerHack();
        }
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    private static class RecoveryTestsArgumentsSource implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
            return Stream.of( arguments( false, false ), arguments( true, false ), arguments( false, true ), arguments( true, true ) );
        }
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

    @ParameterizedTest
    @ArgumentsSource( RecoveryTestsArgumentsSource.class )
    @DisplayName( "Close recovery connection" )
    void closeRecoveryConnection(boolean recoveryCredentials, boolean ignoreRecoveryFlags) throws Exception {
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
                                .connectionProviderClass( RequiresCloseXADataSource.class )
                                .recoveryPrincipal( recoveryCredentials ? new NamePrincipal( RecoveryCredentialsXADataSource.RECOVERY_USER ) : null )
                                .jdbcProperty( "ignoreRecoveryFlags", String.valueOf( ignoreRecoveryFlags ) ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new WarningsAgroalDatasourceListener() ) ) {
            logger.info( "Starting recovery on DataSource " + dataSource );
            recoveryManager.scan();
            logger.info( "Performed first scan. Performing a second scan" );
            recoveryManager.scan();
            logger.info( "Two recovery scans completed" );

            if ( recoveryCredentials ) {
                assertEquals( 2, RequiresCloseXADataSource.getClosed(), "Expected two connections to have been closed" );
            } else {
                assertEquals( 0, RequiresCloseXADataSource.getClosed(), "Expected connections to remain open" );
            }
        }

        if ( recoveryCredentials ) {
            assertEquals( 2, RequiresCloseXADataSource.getClosed(), "Expected two connections to have been closed" );
        } else {
            assertEquals( 1, RequiresCloseXADataSource.getClosed(), "Expected connection closed" );
        }
    }

    @ParameterizedTest
    @ArgumentsSource( RecoveryTestsArgumentsSource.class )
    @DisplayName( "XAResource reopen test" )
    void xaResourceReopenTest(boolean recoveryCredentials, boolean ignoreRecoveryFlags) throws SQLException {
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
                                .connectionProviderClass( RequiresCloseXADataSource.class )
                                .recoveryPrincipal( recoveryCredentials ? new NamePrincipal( RecoveryCredentialsXADataSource.RECOVERY_USER ) : null )
                                .jdbcProperty( "ignoreRecoveryFlags", String.valueOf( ignoreRecoveryFlags ) ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new WarningsAgroalDatasourceListener() ) ) {
            logger.info( "Starting recovery on DataSource " + dataSource );
            recoveryManager.scan();

            if ( recoveryCredentials ) {
                assertEquals( 2, RequiresCloseXADataSource.getClosed(), "Expected two connections to have been closed" );
            } else {
                assertEquals( 0, RequiresCloseXADataSource.getClosed(), "Expected connections to remain open" );
            }
        } finally {
            recoveryManager.terminate( true );
        }
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    @DisplayName( "Eager recovery module test" )
    void eagerRecoveryModuleTest(boolean recoveryCredentials) throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        com.arjuna.ats.arjuna.common.recoveryPropertyManager.getRecoveryEnvironmentBean().setRecoveryBackoffPeriod( 1 );
        RecoveryManager recoveryManager = RecoveryManager.manager( DIRECT_MANAGEMENT );
        recoveryManager.removeAllModules( true );
        recoveryManager.addModule( new EagerRecoveryModule() );

        RecoveryManagerService recoveryService = new RecoveryManagerService();
        recoveryService.create();
        NonEmptyXidsResource.resetRollbackCount();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry, "", false, recoveryService ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .recoveryPrincipal( recoveryCredentials ? new NamePrincipal( RecoveryCredentialsXADataSource.RECOVERY_USER ) : null )
                                .connectionProviderClass( RequiresCloseXADataSource.class ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new WarningsAgroalDatasourceListener() ) ) {
            logger.info( "Starting recovery on DataSource " + dataSource );
            recoveryManager.scan();
            assertEquals( NonEmptyXidsResource.XID_COUNT, NonEmptyXidsResource.rollbackCount(), "Expect action on Xids" );

            if ( recoveryCredentials ) {
                assertEquals( 1 + NonEmptyXidsResource.XID_COUNT, RequiresCloseXADataSource.getClosed(), "Expected two connections to have been closed" );
            } else {
                assertEquals( 0, RequiresCloseXADataSource.getClosed(), "Expected connections to remain open" );
            }
        } finally {
            recoveryManager.terminate( true );
        }
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    @DisplayName( "Leak resource test" )
    void leakingRecoveryTest(boolean poolless) throws SQLException {
        int LEAK_TIMEOUT_S = 1;

        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        com.arjuna.ats.arjuna.common.recoveryPropertyManager.getRecoveryEnvironmentBean().setRecoveryBackoffPeriod( 1 );
        RecoveryManager recoveryManager = RecoveryManager.manager( DIRECT_MANAGEMENT );
        recoveryManager.removeAllModules( true );
        recoveryManager.addModule( new BogusRecoveryModule() );

        RecoveryManagerService recoveryService = new RecoveryManagerService();
        recoveryService.create();
        NonEmptyXidsResource.resetRollbackCount();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .dataSourceImplementation( poolless ? AGROAL_POOLLESS : AGROAL )
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .leakTimeout( Duration.ofSeconds( LEAK_TIMEOUT_S ) )
                        .enhancedLeakReport()
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry, "", false, recoveryService ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .trackJdbcResources( true )
                                .recoveryPrincipal( null )
                                .connectionProviderClass( RequiresCloseXADataSource.class ) )
                );

        LeakDataSourceListener leakListener = new LeakDataSourceListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, leakListener ) ) {
            logger.info( "Starting recovery on DataSource " + dataSource );
            recoveryManager.scan();
            assertEquals( NonEmptyXidsResource.XID_COUNT, NonEmptyXidsResource.rollbackCount(), "Expect action on Xids" );

            if ( !poolless ) {
                assertTrue( leakListener.awaitLeak( 2 * LEAK_TIMEOUT_S, SECONDS ), "Expected warning" );
                assertEquals( 0, RequiresCloseXADataSource.getClosed(), "Expected connections to remain open" );
            }

            dataSource.flush( AgroalDataSource.FlushMode.LEAK );
            assertTrue( leakListener.awaitClose( 2 * LEAK_TIMEOUT_S, SECONDS ), "Expected connection to be flush after leak" );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
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

    private static class LeakDataSourceListener implements AgroalDataSourceListener {

        private final CountDownLatch leakLatch = new CountDownLatch( 1 ), closeLatch = new CountDownLatch( 1 );

        @Override
        public void onConnectionLeak(Connection connection, Thread thread) {
            logger.info( "LEAK of connection " + connection + " by thread " + thread );
            leakLatch.countDown();
        }

        @Override
        public void onConnectionDestroy(Connection connection) {
            logger.info( "DESTROY of connection " + connection );
            closeLatch.countDown();
        }

        @Override
        public void onInfo(String message) {
            logger.info( "leak message: " + message );
        }

        public boolean awaitLeak(long timeout, TimeUnit unit) throws InterruptedException {
            return leakLatch.await( timeout, unit );
        }

        public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
            return closeLatch.await( timeout, unit );
        }
    }

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
        private boolean ignoreRecoveryFlags;

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

        public void setIgnoreRecoveryFlags(boolean ignoreFlags) {
            ignoreRecoveryFlags = ignoreFlags;
        }

        @Override
        public XAConnection getXAConnection() throws SQLException {
            logger.info( "Creating new XAConnection" );
            return new MyMockXAConnection( ignoreRecoveryFlags );
        }

        public static void setUser(String name) {
            logger.info( "Setting username: " + name );
        }

        private static class MyMockXAConnection implements MockXAConnection {

            private final boolean ignoreRecoveryFlags;

            MyMockXAConnection(boolean ignoreFlags) {
                ignoreRecoveryFlags = ignoreFlags;
            }

            @Override
            public XAResource getXAResource() throws SQLException {
                return new NonEmptyXidsResource( ignoreRecoveryFlags );
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
        public static final int XID_COUNT = 5;
        private static int rollbackCount;

        private final List<Xid> xidArray = IntStream.range( 0, XID_COUNT ).mapToObj( ignored -> new XidImple() ).collect( toList() );
        private final boolean ignoreRecoveryFlags;

        public NonEmptyXidsResource(boolean ignoreFlags) {
            ignoreRecoveryFlags = ignoreFlags;
        }

        public static void resetRollbackCount() {
            rollbackCount = 0;
        }

        public static int rollbackCount() {
            return rollbackCount;
        }

        @Override
        public Xid[] recover(int flags) throws XAException {
            return flags == TMENDRSCAN && !ignoreRecoveryFlags ? null : xidArray.toArray( Xid[]::new );
        }

        @Override
        public void forget(Xid xid) throws XAException {
            xidArray.remove( xid );
        }

        @Override
        public void rollback(Xid xid) throws XAException {
            rollbackCount++;
        }
    }

    // --- //

    private static class DoublePassXARecoveryModule extends XARecoveryModule {

        private final Collection<XAResource> xaResources = new ArrayList<>();
        private final Collection<XAResourceRecoveryHelper> xaResourceRecoveryHelpers = new ArrayList<>();
        private int passCount;

        public void addXAResourceRecoveryHelper(XAResourceRecoveryHelper xaResourceRecoveryHelper) {
            xaResourceRecoveryHelpers.add( xaResourceRecoveryHelper );
        }

        public synchronized void removeXAResourceRecoveryHelper(XAResourceRecoveryHelper xaResourceRecoveryHelper) {
            xaResourceRecoveryHelpers.remove( xaResourceRecoveryHelper );
        }

        @Override
        public synchronized void periodicWorkFirstPass() {
            passCount++;
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
            passCount++;
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
                    assertTrue( passCount > 1 || recoveryXids != null && recoveryXids.length > 0, "Expecting some Xids on first pass" );

                    for ( Xid xid : recoveryXids ) {
                        xaResource.forget( xid );
                    }

                    recoveryXids = xaResource.recover( TMENDRSCAN );
                    assertTrue( recoveryXids == null || recoveryXids.length == 0, "Expecting empty Xids" );
                } catch ( XAException e ) {
                    fail( "Failure while using XA Resource", e );
                }
            }
        }
    }

    private static class EagerRecoveryModule extends XARecoveryModule {

        private final Collection<XAResourceRecoveryHelper> xaResourceRecoveryHelpers = new ArrayList<>();

        public void addXAResourceRecoveryHelper(XAResourceRecoveryHelper xaResourceRecoveryHelper) {
            xaResourceRecoveryHelpers.add( xaResourceRecoveryHelper );
        }

        public synchronized void removeXAResourceRecoveryHelper(XAResourceRecoveryHelper xaResourceRecoveryHelper) {
            xaResourceRecoveryHelpers.remove( xaResourceRecoveryHelper );
        }

        @Override
        public synchronized void periodicWorkFirstPass() {
            for ( XAResourceRecoveryHelper helper : xaResourceRecoveryHelpers ) {
                try {
                    for ( XAResource xaResource : helper.getXAResources() ) {
                        for ( Xid xid : xaResource.recover( TMSTARTRSCAN | TMENDRSCAN ) ) {
                            xaResource.rollback( xid );
                        }
                    }
                } catch ( Exception e ) {
                    fail( "Failure while obtaining XA Resource", e );
                }
            }
        }
    }

    private static class BogusRecoveryModule extends XARecoveryModule {

        private final Collection<XAResourceRecoveryHelper> xaResourceRecoveryHelpers = new ArrayList<>();

        public void addXAResourceRecoveryHelper(XAResourceRecoveryHelper xaResourceRecoveryHelper) {
            xaResourceRecoveryHelpers.add( xaResourceRecoveryHelper );
        }

        public synchronized void removeXAResourceRecoveryHelper(XAResourceRecoveryHelper xaResourceRecoveryHelper) {
            xaResourceRecoveryHelpers.remove( xaResourceRecoveryHelper );
        }

        @Override
        public synchronized void periodicWorkFirstPass() {
            for ( XAResourceRecoveryHelper helper : xaResourceRecoveryHelpers ) {
                try {
                    for ( XAResource xaResource : helper.getXAResources() ) {
                        for ( Xid xid : xaResource.recover( TMSTARTRSCAN ) ) {
                            xaResource.rollback( xid );
                        }
                    }
                } catch ( Exception e ) {
                    fail( "Failure while obtaining XA Resource", e );
                }
            }
        }
    }
}
