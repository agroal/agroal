// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

/**
 * JDK 21+ override — uses Thread.isVirtual() directly.
 */
public final class VirtualThreadUtil {

    private VirtualThreadUtil() {
    }

    public static boolean isVirtualThread() {
        return Thread.currentThread().isVirtual();
    }
}
