// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.osgi;

import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.pool.DataSource;
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
    public static void setupMockDriver() {
        registerMockDriver();
    }

    @AfterAll
    public static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "test deployment on OSGi container" )
    public void basicOSGiConnectionAcquireTest() throws Exception {
        ExamSystem examSystem = PaxExamRuntime.createTestSystem(
                CoreOptions.systemProperty( "org.ops4j.pax.logging.DefaultServiceLog.level" ).value( "WARN" ),
                CoreOptions.mavenBundle().groupId( "io.agroal" ).artifactId( "agroal-api" ).versionAsInProject(),
                CoreOptions.mavenBundle().groupId( "io.agroal" ).artifactId( "agroal-pool" ).versionAsInProject()
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
    public static class AgroalProbe {

        private static final Logger probeLogger = getLogger( AgroalProbe.class.getName() );

        private static TestProbeBuilder getTestProbeBuilder(ExamSystem examSystem) throws IOException {
            TestProbeBuilder testProbeBuilder = examSystem.createProbe();
            testProbeBuilder.addTest( AgroalProbe.class );
            return testProbeBuilder;
        }

        /**
         * AgroalDataSource.from( ... ) wont't work due to limitations of ServiceLoader on OSGi environments.
         * For this test, and OSGi deployments in general, the datasource implementation is instantiated directly.
         */
        public void probe(BundleReference bundleReference) throws SQLException {
            probeLogger.info( "In OSGi container running from a Bundle named " + bundleReference.getBundle().getSymbolicName() );

            try ( DataSource dataSource = new DataSource( new AgroalDataSourceConfigurationSupplier().get() ) ) {
                try ( Connection connection = dataSource.getConnection() ) {
                    probeLogger.info( format( "Got connection {0}", connection ) );
                }
            }
        }
    }
}

