// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import java.io.Serializable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Virtual-thread friendly synchronizer
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 * @author <a href="matt.gdefreitas@gmail.com">Limcon</a>
 */
public final class AgroalSynchronizer implements Serializable {

    private static final long serialVersionUID = -57548578257544072L;

    private final AtomicLong released = new AtomicLong(0);
    private final Semaphore signal = new Semaphore(0, true);

    public long getStamp() {
        return released.get();
    }

    // Try to acquire permission (used to check if a connection can proceed)
    public boolean tryAcquire(long stamp) {
        return released.get() > stamp;
    }

    // Blocking wait with timeout (used by the connection pool to wait for availability)
    public boolean tryAcquireNanos(long stamp, long nanosTimeout) throws InterruptedException {
        if (released.get() > stamp) {
            return true;
        }
        return signal.tryAcquire(nanosTimeout, TimeUnit.NANOSECONDS);
    }

    // Release signal (used when a connection is returned)
    public void release() {
        released.incrementAndGet();
        signal.release();
    }

    // Release multiple signals
    public void release(int amount) {
        released.addAndGet( amount );
        signal.release( amount );
    }

    // Release only if someone is waiting
    public void releaseConditional() {
        if ( signal.hasQueuedThreads() ) {
            release();
        }
    }

    // Get the amount of threads waiting
    public int getQueueLength() {
        return signal.getQueueLength();
    }
}
