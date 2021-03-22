// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.osgi;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.agroal.test.MockConnection;
import io.agroal.test.MockDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.ExamSystem;
import org.ops4j.pax.exam.TestAddress;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.spi.DefaultExamReactor;
import org.ops4j.pax.exam.spi.ExamReactor;
import org.ops4j.pax.exam.spi.PaxExamRuntime;
import org.ops4j.pax.exam.spi.StagedExamReactor;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.osgi.framework.BundleReference;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.OSGI;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.util.logging.Logger.getLogger;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( OSGI )
public class BasicOSGiTests {

    private static final Logger logger = getLogger( BasicOSGiTests.class.getName() );

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
    @DisplayName( "test deployment on OSGi container" )
    void basicOSGiConnectionAcquireTest() throws Exception {
        ExamSystem examSystem = PaxExamRuntime.createTestSystem(
                CoreOptions.systemProperty( "org.ops4j.pax.logging.DefaultServiceLog.level" ).value( "WARN" ),
                CoreOptions.mavenBundle().groupId( "io.agroal" ).artifactId( "agroal-api" ).versionAsInProject(),
                CoreOptions.mavenBundle().groupId( "io.agroal" ).artifactId( "agroal-pool" ).versionAsInProject(),
                CoreOptions.mavenBundle().groupId( "io.agroal" ).artifactId( "agroal-test" ).versionAsInProject()
        );
        ExamReactor examReactor = new DefaultExamReactor( examSystem, PaxExamRuntime.getTestContainerFactory() );
        examReactor.addProbe( AgroalProbe.getTestProbeBuilder( examSystem ) );

        logger.info( "Starting OSGi container ..." );

        StagedExamReactor stagedReactor = examReactor.stage( new PerClass() );
        stagedReactor.beforeClass();
        try {
            for ( TestAddress call : stagedReactor.getTargets() ) {
                stagedReactor.invoke( call );
            }
            logger.info( "Stopping OSGi container after successful invocation" );
        } finally {
            stagedReactor.afterClass();
        }
    }

    // --- //

    /**
     * This class is turned into a Bundled and then deployed to the OSGi container
     */
    @SuppressWarnings( {"UtilityClass", "WeakerAccess"} )
    private static final class AgroalProbe {

        static final Logger probeLogger = getLogger( AgroalProbe.class.getName() );

        static TestProbeBuilder getTestProbeBuilder(ExamSystem examSystem) throws IOException {
            TestProbeBuilder testProbeBuilder = examSystem.createProbe();
            testProbeBuilder.addTest( AgroalProbe.class );
            return testProbeBuilder;
        }

        /**
         * AgroalDataSource.from( ... ) won't work due to limitations of ServiceLoader on OSGi environments.
         * For this test, and OSGi deployments in general, the datasource implementation is instantiated directly.
         */
        @SuppressWarnings( "unused" )
        public static void probe(BundleReference bundleReference) throws SQLException {
            probeLogger.info( "In OSGi container running from a Bundle named " + bundleReference.getBundle().getSymbolicName() );

            AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                    .connectionPoolConfiguration( cp -> cp
                            .maxSize( 10 )
                            .connectionFactoryConfiguration( cf -> cf
                                    .connectionProviderClass( CredentialsDataSource.class )
                                    .principal( new NamePrincipal( CredentialsDataSource.DEFAULT_USER ) )
                                    .credential( new SimplePassword( CredentialsDataSource.DEFAULT_PASSWORD ) )
                            )
                    );
            try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
                try ( Connection connection = dataSource.getConnection() ) {
                    probeLogger.info( format( "Got connection {0}", connection ) );
                }
            }
        }
    }

    // --- //

    @SuppressWarnings( "unused" )
    public static class CredentialsDataSource implements MockDataSource {

        private static final String DEFAULT_USER = "def_user";
        private static final String DEFAULT_PASSWORD = "def_pass";

        private String user;
        private String password;

        public void setUser(String user) {
            this.user = user;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        @Override
        @SuppressWarnings( "CallToSuspiciousStringMethod" )
        public Connection getConnection() throws SQLException {
            if ( !DEFAULT_USER.equals( user ) ) {
                throw new RuntimeException( "Expecting user '" + DEFAULT_USER + "' but got '" + user + "' instead" );
            }
            if ( !DEFAULT_PASSWORD.equals( password ) ) {
                throw new RuntimeException( "Expecting password '" + DEFAULT_PASSWORD + "' but got '" + password + "' instead" );
            }
            AgroalProbe.probeLogger.info( format( "Connection with username:{0} and password:{1}", user, password ) );
            return new MockConnection.Empty();
        }
    }
}

