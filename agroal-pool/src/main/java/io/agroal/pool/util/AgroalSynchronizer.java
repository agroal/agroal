// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.AbstractQueuedLongSynchronizer;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalSynchronizer extends AbstractQueuedLongSynchronizer {

    private final LongAdder counter = new LongAdder();

    @Override
    protected boolean tryAcquire(long value) {
//        if ( counter.longValue() > value) System.out.printf( "     >>>   %s got UNLOCKED!!  (%d > %d)%n", Thread.currentThread().getName(), counter.longValue(), value );
//        else System.out.printf( "  ------  %s got LOCKED on __ %d __ (current %d) %n", Thread.currentThread().getName(), value, counter.longValue() );

        // Advance when counter is greater than value
        return counter.longValue() > value;
    }

    @Override
    protected boolean tryRelease(long releases) {
//        System.out.printf( "  >>>     %s releases __ %d __%n", Thread.currentThread().getName(), counter.longValue() );

        counter.add( releases );
        return true;
    }

    // --- //

    public long getStamp() {
        return counter.sum();
    }

    public void releaseConditional() {
        if ( hasQueuedThreads() ) {
            release( 1 );
        }
    }
}
