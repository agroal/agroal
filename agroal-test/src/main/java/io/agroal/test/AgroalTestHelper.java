// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test;

import java.time.Duration;

import static java.lang.System.nanoTime;
import static java.lang.Thread.sleep;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalTestHelper {

    private AgroalTestHelper() {
    }

    public static void safeSleep(Duration duration) {
        long target = nanoTime() + duration.toNanos();
        do {
            try {
                sleep( duration.toMillis() );
            } catch ( InterruptedException e ) {
                // retry
            }
        } while ( nanoTime() < target );
    }
}
