package io.agroal.pool;

import java.util.concurrent.Callable;

public interface CreateConnectionCallable extends Callable<ConnectionHandler> {
}