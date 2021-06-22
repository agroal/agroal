// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.cache;

/**
 * Interface for a cache of connections. It's intended for mapping connections to executing threads.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface ConnectionCache {

    /**
     * An implementation that does not cache.
     */
    static ConnectionCache none() {
        return new ConnectionCache() {
            @Override
            public Acquirable get() {
                return null;
            }

            @Override
            public void put(Acquirable acquirable) {
                // nothing to do
            }

            @Override
            public void reset() {
                // nothing to do
            }
        };
    }

    // --- //

    /**
     * Get a acquirable object from cache.
     *
     * @return a connection successfully acquired, according to {@link Acquirable#acquire()}
     */
    Acquirable get();

    /**
     * Cache an acquirable object on this cache.
     */
    void put(Acquirable acquirable);

    /**
     * Reset the cache.
     */
    void reset();
}
