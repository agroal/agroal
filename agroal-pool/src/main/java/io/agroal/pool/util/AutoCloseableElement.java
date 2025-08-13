// Copyright (C) 2023 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * Abstract class to track auto-closeable resources by having them forming a collection between themselves.
 * This class is designed to prevent leaks of Agroal JDBC wrapper objects.
 * Once a new wrapper is created it tries to insert itself between the head and the first element of the list (if it exists).
 * There is the invariant that at any given point in time the list can be traversed from the head and all inserted elements are reachable.
 * As an implementation detail, the collection formed is actually a stack (FILO behaviour) and is thread-safe.
 * <p>
 * The resources do not remove themselves on close, rather there is a {@link #pruneClosed()} method that must be called to remove subsequent closed elements.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public abstract class AutoCloseableElement implements AutoCloseable {

    private static final AtomicReferenceFieldUpdater<AutoCloseableElement, AutoCloseableElement> NEXT_UPDATER = newUpdater( AutoCloseableElement.class, AutoCloseableElement.class, "nextElement" );

    private volatile AutoCloseableElement nextElement;

    public abstract boolean isClosed() throws Exception;

    protected abstract boolean internalClosed();

    @SuppressWarnings( "ThisEscapedInObjectConstruction" )
    protected AutoCloseableElement(AutoCloseableElement head) {
        if ( head != null ) {
            // point to the first element of the list and attempt to have the head to point at us
            do {
                nextElement = head.getNextElement();
            } while ( !head.setNextElement( nextElement, this ) );
        }
    }

    /**
     * Returns the number of resources that were not properly closed. The resources are closed in the process and the collection is cleared.
     * This method should be invoked on the collection head only, otherwise it may not traverse the whole collection.
     */
    public int closeAllAutocloseableElements() {
        int count = 0;
        // if under contention, the call that succeeds to reset the head is the one and only that will traverse the whole collection
        for ( AutoCloseableElement next = resetNextElement(); next != null; next = next.resetNextElement() ) {
            try {
                if ( !next.internalClosed() ) {
                    count++;
                    if ( next instanceof Statement ) {
                        try {
                            // AG-231 - we have to cancel the Statement on cleanup to avoid overloading the DB
                            ( (Statement) next ).cancel();
                        } catch ( SQLException e ) {
                            // ignore and proceed with close()
                        }
                    }
                    next.close();
                }
            } catch ( Exception e ) {
                // ignore
            }
        }
        return count;
    }

    // --- convenience private methods to access the field updater //

    private boolean setNextElement(AutoCloseableElement expected, AutoCloseableElement element) {
        return NEXT_UPDATER.compareAndSet( this, expected, element );
    }

    private AutoCloseableElement getNextElement() {
        return NEXT_UPDATER.get( this );
    }

    private AutoCloseableElement resetNextElement() {
        return NEXT_UPDATER.getAndSet( this, null );
    }

    /**
     * Check for runs of closed elements after the current position and remove them from the linked list
     */
    public void pruneClosed() {
        AutoCloseableElement next = nextElement;
        while (next != null && next.internalClosed() && setNextElement(next, next.nextElement)) {
            next = nextElement;
        }
    }

    // --- head of the list //

    /**
     * Create a special marker element to be used as head of a collection.
     */
    public static AutoCloseableElement newHead() {
        return new AutoCloseableElementHead();
    }

    private static class AutoCloseableElementHead extends AutoCloseableElement {

        private AutoCloseableElementHead() {
            super( null );
        }

        @Override
        public boolean isClosed() {
            throw new IllegalStateException();
        }

        @Override
        public void close() throws Exception {
            throw new IllegalStateException();
        }

        @Override
        protected boolean internalClosed() {
            return false;
        }
    }
}
