// Copyright (C) 2023 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test;

import org.junit.platform.launcher.LauncherInterceptor;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.logging.Logger;

import static java.lang.Thread.currentThread;
import static java.util.logging.Logger.getLogger;

/**
 * {@link LauncherInterceptor} that loads test classes, and classes the test relies on from designated packages, in new {@link ClassLoader}.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@SuppressWarnings( "unused" )
public class IsolatingClassloaderLauncherInterceptor implements LauncherInterceptor {

    private static final String TRANSACTION_TEST_PACKAGE_PREFIX = "io.agroal.test.narayana", NARAYANA_PACKAGE_PREFIX = "com.arjuna";

    private final ClassLoader parentClassloader;

    public IsolatingClassloaderLauncherInterceptor() {
        parentClassloader = currentThread().getContextClassLoader();
    }

    @Override
    public <T> T intercept(Invocation<T> invocation) {
        currentThread().setContextClassLoader( new IsolatingClassLoader( parentClassloader ) );
        return invocation.proceed();
    }

    @Override
    public void close() {
        currentThread().setContextClassLoader( parentClassloader );
    }

    // --- //

    // This classloader intercepts calls to loadClass() and redefines the test class in a new classloader
    private static class IsolatingClassLoader extends ClassLoader {

        private static final Logger logger = getLogger( IsolatingClassLoader.class.getName() );

        public IsolatingClassLoader(ClassLoader parent) {
            super( parent );
        }

        @Override
        @SuppressWarnings( {"StringConcatenation", "HardcodedFileSeparator"} )
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if ( name.startsWith( TRANSACTION_TEST_PACKAGE_PREFIX ) && !name.contains( "$" ) ) {
                Class<?> existingClass = findLoadedClass( name );
                URL resourceURL = getResource( existingClass != null ? existingClass.getSimpleName() : name.replace( ".", "/" ) + ".class" );
                if ( resourceURL != null ) {
                    try ( InputStream in = resourceURL.openStream() ) {
                        logger.info( "New NarayanaRedefiningClassloader for test class " + name );
                        return new NarayanaRedefiningClassloader( getParent() ).defineClass( name, in.readAllBytes() );
                    } catch ( IOException e ) {
                        throw new RuntimeException( e );
                    }
                }
            }
            return super.loadClass( name, resolve );
        }
    }

    // This classloader re-defines the test case (including inner classes) and all the Narayana classes
    // Loading of other classes is delegated to the parent classloader
    private static class NarayanaRedefiningClassloader extends ClassLoader {

        public NarayanaRedefiningClassloader(ClassLoader parent) {
            super( parent );
        }

        @Override
        @SuppressWarnings( "HardcodedFileSeparator" )
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if ( name.startsWith( TRANSACTION_TEST_PACKAGE_PREFIX ) || name.startsWith( NARAYANA_PACKAGE_PREFIX ) ) {
                Class<?> existingClass = findLoadedClass( name );
                URL resourceURL = getResource( ( existingClass != null ) ? existingClass.getSimpleName() : name.replace( ".", "/" ) + ".class" );
                if ( resourceURL != null ) {
                    try ( InputStream in = resourceURL.openStream() ) {
                        defineClass( name, in.readAllBytes() );
                    } catch ( IOException e ) {
                        throw new RuntimeException( e );
                    }
                }
            }
            return super.loadClass( name, resolve );
        }

        public Class<?> defineClass(String name, byte[] data) {
            return defineClass( name, data, 0, data.length );
        }
    }
}
