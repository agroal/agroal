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
public final class InterceptorHelper {

    private InterceptorHelper() {
    }

    public static void fireOnConnectionAcquiredInterceptor(List<AgroalPoolInterceptor> interceptors, ConnectionHandler handler) throws SQLException {
        if ( interceptors != null && interceptors.size() > 0 ) {
            for ( AgroalPoolInterceptor interceptor : interceptors ) {
                try ( Connection connection = handler.newDetachedConnectionWrapper() ) {
                    interceptor.onConnectionAcquire( connection );
                }
            }
        }
    }

    public static void fireOnConnectionReturnInterceptor(List<AgroalPoolInterceptor> interceptors, ConnectionHandler handler) throws SQLException {
        if ( interceptors != null && interceptors.size() > 0 ) {
            for ( int i = interceptors.size(); i > 0; ) {
                try ( Connection connection = handler.newDetachedConnectionWrapper() ) {
                    interceptors.get( --i ).onConnectionReturn( connection );
                }
            }
        }
    }
}
