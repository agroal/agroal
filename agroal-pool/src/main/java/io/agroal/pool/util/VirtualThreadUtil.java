// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import java.util.concurrent.ExecutorService;

/**
 * Utility for virtual thread detection. On JDK < 21 this always returns false.
 * On JDK 21+ the multi-release JAR overrides this with Thread.isVirtual().
 */
public final class VirtualThreadUtil {

    private VirtualThreadUtil() {
    }

    public static boolean isVirtualThread() {
        return false;
    }

    public static ExecutorService newVirtualThreadPerTaskExecutor() {
        throw new UnsupportedOperationException();
    }
}
