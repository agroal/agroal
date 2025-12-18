// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import static java.lang.System.identityHashCode;
import static java.sql.DriverManager.deregisterDriver;
import static java.sql.DriverManager.getDriver;
import static java.sql.DriverManager.registerDriver;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Logger.getLogger;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface MockDriver extends Driver {

    Logger logger = getLogger( MockDriver.class.getName() );

    DriverPropertyInfo[] EMPTY_PROPERTY_INFO = new DriverPropertyInfo[0];

    static void registerMockDriver(Class<? extends Connection> connectionType) {
        registerMockDriver( new MockDriver.Empty( connectionType ) );
    }

    static void registerMockDriver(Driver... drivers) {
        registerMockDriver( new MultiStepMockDriver( List.of( drivers ) ) );
    }

    static void registerMockDriver(Driver driver) {
        try {
            registerDriver( driver );
        } catch ( SQLException e ) {
            getLogger( MockDriver.class.getName() ).log( WARNING, "Unable to register MockDriver into Driver Manager", e );
        }
    }

    static void registerMockDriver() {
        registerMockDriver( MockConnection.Empty.class );
    }

    static void deregisterMockDriver() {
        Driver driver = null;
        try {
            driver = getDriver( "" );
        } catch ( SQLException e ) {
            if ( !"No suitable driver".equals( e.getMessage() ) ) {
                throw new RuntimeException( "Unexpected Exception during getDriver", e );
            }
        }

        try {
            deregisterDriver( driver );
        } catch ( SQLException e ) {
            throw new RuntimeException( "Unable to deregister MockDriver from Driver Manager", e );
        }
    }

    // --- //

    @Override
    default Connection connect(String url, Properties info) throws SQLException {
        return null;
    }

    @Override
    default boolean acceptsURL(String url) throws SQLException {
        return true;
    }

    @Override
    default DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return EMPTY_PROPERTY_INFO;
    }

    @Override
    default int getMajorVersion() {
        return 0;
    }

    @Override
    default int getMinorVersion() {
        return 0;
    }

    @Override
    default boolean jdbcCompliant() {
        return false;
    }

    @Override
    default Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    class Empty implements MockDriver {

        Class<? extends Connection> connectionType;

        public Empty() {
            this( MockConnection.Empty.class );
        }

        public Empty(Class<? extends Connection> connectionType) {
            this.connectionType = connectionType;
        }

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            try {
                return connectionType.getDeclaredConstructor().newInstance();
            } catch ( InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e ) {
                throw new SQLException( "Cannot create mock connection", e );
            }
        }

        @Override
        public String toString() {
            return "MockDriver@" + identityHashCode( this );
        }
    }
}
