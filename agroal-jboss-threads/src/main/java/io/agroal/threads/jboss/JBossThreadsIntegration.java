// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.threads.jboss;

import io.agroal.api.configuration.InterruptProtection;
import org.jboss.threads.JBossThread;

import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class JBossThreadsIntegration implements InterruptProtection {

    public <T> T protect(SQLCallable<T> callable) throws SQLException {
        try {
            return JBossThread.executeWithInterruptDeferred( (Callable<T>) callable::call );
        } catch ( SQLException e ) {
            throw e;
        } catch ( Exception e ) {
            throw new SQLException( "Exception while executing protected call", e );
        }
    }
}
