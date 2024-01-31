// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import io.agroal.api.AgroalPoolInterceptor;
import io.agroal.pool.ConnectionHandler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@SuppressWarnings( {"UtilityClass", "ObjectAllocationInLoop"} )
public final class InterceptorHelper {

    private InterceptorHelper() {
    }

    public static void fireOnConnectionCreateInterceptor(List<? extends AgroalPoolInterceptor> interceptors, ConnectionHandler handler) throws SQLException {
        if ( interceptors != null && !interceptors.isEmpty() ) {
            for ( AgroalPoolInterceptor interceptor : interceptors ) {
                try ( Connection connection = handler.detachedWrapper() ) {
                    interceptor.onConnectionCreate( connection );
                }
            }
        }
    }

    public static void fireOnConnectionAcquiredInterceptor(List<? extends AgroalPoolInterceptor> interceptors, ConnectionHandler handler) throws SQLException {
        if ( interceptors != null && !interceptors.isEmpty() ) {
            for ( AgroalPoolInterceptor interceptor : interceptors ) {
                try ( Connection connection = handler.detachedWrapper() ) {
                    interceptor.onConnectionAcquire( connection );
                }
            }
        }
    }

    public static void fireOnConnectionReturnInterceptor(List<? extends AgroalPoolInterceptor> interceptors, ConnectionHandler handler) throws SQLException {
        if ( interceptors != null && !interceptors.isEmpty() ) {
            for ( int i = interceptors.size(); i > 0; ) {
                try ( Connection connection = handler.detachedWrapper() ) {
                    interceptors.get( --i ).onConnectionReturn( connection );
                }
            }
        }
    }

    public static void fireOnConnectionDestroyInterceptor(List<? extends AgroalPoolInterceptor> interceptors, ConnectionHandler handler) throws SQLException {
        if ( interceptors != null && !interceptors.isEmpty() ) {
            for ( int i = interceptors.size(); i > 0; ) {
                try ( Connection connection = handler.detachedWrapper() ) {
                    interceptors.get( --i ).onConnectionDestroy( connection );
                }
            }
        }
    }
}
