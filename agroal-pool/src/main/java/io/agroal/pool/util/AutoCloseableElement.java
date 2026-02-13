// Copyright (C) 2023 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import io.agroal.pool.wrapper.ConnectionWrapper;

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
public abstract class AutoCloseableElement<T extends AutoCloseableElement<T>> implements AutoCloseable {

    @SuppressWarnings( "rawtypes" )
    private static final AtomicReferenceFieldUpdater<AutoCloseableElement, AutoCloseableElement> NEXT_UPDATER = newUpdater( AutoCloseableElement.class, AutoCloseableElement.class, "nextElement" );

    private volatile AutoCloseableElement<T> nextElement;

    public boolean isHeld() {
        return true;
    }

    public abstract boolean isClosed() throws Exception;

    protected abstract boolean internalClosed();

    protected void beforeClose() {
    }

    @SuppressWarnings( "ThisEscapedInObjectConstruction" )
    protected AutoCloseableElement(AutoCloseableElement<T> head) {
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
        // To be called on the head of the list
        throw new IllegalStateException();
    }

    /**
     * Returns the number of resources that were not held and are not properly closed.
     * After run the resources that are not held are closed and remove from the list.
     * This method should be invoked on the collection head only, otherwise it may not traverse the whole collection.
     */
    public int closeNotHeldAutocloseableElements() {
        // To be called on the head of the list
        throw new IllegalStateException();
    }

    /**
     * Check if the list of elements is empty
     */
    public boolean isElementListEmpty() {
        // To be called on the head of the list
        throw new IllegalStateException();
    }

    // --- convenience protected methods to access the field updater //

    protected boolean setNextElement(AutoCloseableElement<T> expected, AutoCloseableElement<T> element) {
        return NEXT_UPDATER.compareAndSet( this, expected, element );
    }

    @SuppressWarnings( "unchecked" )
    protected AutoCloseableElement<T> getNextElement() {
        return NEXT_UPDATER.get( this );
    }

    @SuppressWarnings( "unchecked" )
    protected AutoCloseableElement<T> resetNextElement() {
        return NEXT_UPDATER.getAndSet( this, null );
    }

    /**
     * Check for runs of closed elements after the current position and remove them from the linked list
     */
    public void pruneClosed() {
        AutoCloseableElement<T> next = nextElement;
        while ( next != null && next.internalClosed() && setNextElement( next, next.nextElement ) ) {
            next = nextElement;
        }
    }

    // --- head of the list //

    /**
     * Create a special marker element to be used as head of a collection.
     */
    public static <T extends AutoCloseableElement<T>> AutoCloseableElement<T> newHead() {
        return new AutoCloseableElementHead<>();
    }

    private static class AutoCloseableElementHead<T extends AutoCloseableElement<T>> extends AutoCloseableElement<T> {

        private AutoCloseableElementHead() {
            super( null );
        }

        @Override
        public boolean isElementListEmpty() {
            return getNextElement() == null;
        }

        @Override
        public boolean isHeld() {
            throw new IllegalStateException();
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

        public int closeAllAutocloseableElements() {
            int count = 0;
            // if under contention, the call that succeeds to reset the head is the one and only that will traverse the whole collection
            for ( AutoCloseableElement<T> next = resetNextElement(); next != null; next = next.resetNextElement() ) {
                try {
                    if ( !next.internalClosed() ) {
                        next.beforeClose();
                        next.close();
                        count++;
                    }
                } catch ( Exception e ) {
                    // ignore
                }
            }
            return count;
        }

        public int closeNotHeldAutocloseableElements() {
            int count = 0;
            for ( AutoCloseableElement<T> current = this; current != null && current.nextElement != null; ) {
                AutoCloseableElement<T> next = current.nextElement;
                try {
                    boolean holdElement = next.isHeld() && !next.internalClosed();
                    if ( !holdElement && next instanceof ConnectionWrapper connectionWrapper ) {
                        connectionWrapper.closeNotHeldTrackedStatements();
                        holdElement = !connectionWrapper.hasTrackedStatements();
                    }

                    if ( !holdElement && current.setNextElement( next, next.nextElement ) && !next.internalClosed() ) {
                        next.beforeClose();
                        next.close();
                        count++;
                    } else {
                        current = current.nextElement;
                    }
                } catch ( Exception e ) {
                    // ignore
                }
            }
            return count;
        }
    }
}
