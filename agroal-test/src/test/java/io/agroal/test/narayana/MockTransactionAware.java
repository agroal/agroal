package io.agroal.test.narayana;

import io.agroal.api.transaction.TransactionAware;

import java.sql.SQLException;

import static java.lang.System.identityHashCode;

public interface MockTransactionAware extends TransactionAware {
    @Override
    default void transactionStart() throws SQLException {
    }

    @Override
    default void transactionBeforeCompletion(boolean successful) {
    }

    @Override
    default void transactionCommit() throws SQLException {
    }

    @Override
    default void transactionRollback() throws SQLException {
    }

    @Override
    default void transactionEnd() throws SQLException {
    }

    @Override
    default void transactionCheckCallback(SQLCallable<Boolean> transactionCheck) {
    }

    @Override
    default Object getConnection() {
        return null;
    }

    @Override
    default void setFlushOnly() {
    }

    class Empty implements MockTransactionAware {
        @Override
        public String toString() {
            return "MockTransactionAware@" + identityHashCode( this );
        }
    }
}
