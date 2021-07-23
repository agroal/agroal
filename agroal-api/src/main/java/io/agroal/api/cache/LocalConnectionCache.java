// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.cache;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Implementations of ConnectionCache that rely on {@link ThreadLocal}.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface LocalConnectionCache {

    /**
     * A local cache that stores at most a single connection.
     */
    static ConnectionCache single() {
        return new ConnectionCache() {

            @SuppressWarnings( "ThreadLocalNotStaticFinal" )
            private volatile ThreadLocal<Acquirable> threadLocal;

            { // instance initializer
                reset();
            }

            @Override
            public Acquirable get() {
                Acquirable acquirable = threadLocal.get();
                return acquirable != null && acquirable.acquire() ? acquirable : null;
            }

            @Override
            public void put(Acquirable acquirable) {
                if ( acquirable.isAcquirable() ) {
                    threadLocal.set( acquirable );
                }
            }

            @Override
            public void reset() {
                threadLocal = new ThreadLocal<>();
            }
        };
    }

    /**
     * A local cache that stores up to a number of connections.
     */
    static ConnectionCache fixed(int size) {
        return new ConnectionCache() {

            @SuppressWarnings( "ThreadLocalNotStaticFinal" )
            private volatile ThreadLocal<Acquirable[]> threadLocal;

            { // instance initializer
                reset();
            }

            @Override
            public Acquirable get() {
                Acquirable[] cacheArray = threadLocal.get();
                for ( int i = cacheArray.length; i > 0; ) { // iterate from last to first
                    Acquirable element = cacheArray[--i];
                    if ( element != null ) {
                        cacheArray[i] = null;
                        if ( element.acquire() ) {
                            return element;
                        }
                    }
                }
                return null;
            }

            @Override
            public void put(Acquirable acquirable) {
                if ( acquirable.isAcquirable() ) {
                    Acquirable[] cacheArray = threadLocal.get();
                    int last = cacheArray.length - 1;
                    if ( cacheArray[last] != null ) { // always store in last. if there is a previous entry try to move it to other slot
                        int i = last;
                        while ( --i > 0 && cacheArray[i] != null ) ; // find first free slot
                        cacheArray[i < 0 ? last - 1 : i] = cacheArray[last]; // if no free slot found, override last -1
                    }
                    cacheArray[last] = acquirable;
                }
            }

            @Override
            public void reset() {
                threadLocal = ThreadLocal.withInitial( () -> new Acquirable[size] );
            }
        };
    }

    /**
     * A local cache that stores all connections
     */
    static ConnectionCache full() {
        return new ConnectionCache() {

            @SuppressWarnings( "ThreadLocalNotStaticFinal" )
            private volatile ThreadLocal<Deque<Acquirable>> threadLocal;

            { // instance initializer
                reset();
            }

            @Override
            public Acquirable get() {
                Deque<Acquirable> queue = threadLocal.get();

                for ( int i = queue.size(); i > 0; i-- ) {
                    Acquirable acquirable = queue.removeFirst();
                    if ( acquirable.acquire() ) {
                        return acquirable;
                    } else if ( acquirable.isAcquirable() ) {
                        queue.addLast( acquirable );
                    }
                }
                return null;
            }

            @Override
            public void put(Acquirable acquirable) {
                if ( acquirable.acquire() ) {
                    threadLocal.get().addFirst( acquirable );
                }
            }

            @Override
            public void reset() {
                // ArrayDeque with default initial capacity of 16
                threadLocal = ThreadLocal.withInitial( ArrayDeque::new );
            }
        };
    }
}
