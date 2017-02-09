// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration;

import static java.lang.ClassLoader.getSystemClassLoader;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface ClassLoaderProvider {

    static ClassLoaderProvider systemClassloader() {
        return className -> getSystemClassLoader();
    }

    // --- //

    ClassLoader getClassLoader(String className);
}
