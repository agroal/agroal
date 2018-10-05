// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.transaction;

import java.sql.SQLException;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public interface TransactionAware {

    void transactionStart() throws SQLException;

    void transactionCommit() throws SQLException;

    void transactionRollback() throws SQLException;

    void transactionEnd() throws SQLException;

    void transactionCheckCallback(SQLCallable<Boolean> transactionCheck);

    Object getConnection();

    void setFlushOnly();

    // --- //

    @FunctionalInterface
    interface SQLCallable<T> {

        T call() throws SQLException;
    }
}
