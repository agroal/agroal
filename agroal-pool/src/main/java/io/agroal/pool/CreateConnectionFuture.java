package io.agroal.pool;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public class CreateConnectionFuture extends FutureTask<ConnectionHandler> {

    public CreateConnectionFuture(Callable<ConnectionHandler> callable) {
        super(callable);
    }
}