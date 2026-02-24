// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.lang.System.arraycopy;
import static java.lang.reflect.Array.newInstance;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class StampedCopyOnWriteArrayList<T> implements List<T> {

    private final Iterator<T> emptyIterator = new Iterator<T>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            throw new NoSuchElementException();
        }
    };

    private static final VarHandle DATA_HANDLE;

    static {
        try {
            DATA_HANDLE = MethodHandles.lookup().findVarHandle( StampedCopyOnWriteArrayList.class, "data", Object[].class );
        } catch ( ReflectiveOperationException e ) {
            throw new ExceptionInInitializerError( e );
        }
    }

    private T[] data;

    private final StampedLock lock = new StampedLock();

    // -- //

    @SuppressWarnings( "ThisEscapedInObjectConstruction" )
    public StampedCopyOnWriteArrayList(Class<? extends T> clazz) {
        DATA_HANDLE.setRelease( this, newInstance( clazz, 0 ) );
    }

    @SuppressWarnings( "unchecked" )
    private T[] getUnderlyingArray() {
        return (T[]) DATA_HANDLE.getAcquire( this );
    }

    @Override
    public T get(int index) {
        return getUnderlyingArray()[index];
    }

    @Override
    public int size() {
        return getUnderlyingArray().length;
    }

    // --- //

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean add(T element) {
        long stamp = lock.writeLock();
        try {
            T[] current = getUnderlyingArray();
            T[] array = Arrays.copyOf( current, current.length + 1 );
            array[array.length - 1] = element;
            DATA_HANDLE.setRelease( this, array );
            return true;
        } finally {
            lock.unlockWrite( stamp );
        }
    }

    public T removeLast() {
        long stamp = lock.writeLock();
        try {
            T[] current = getUnderlyingArray();
            if ( current.length == 0 ) {
                throw new NoSuchElementException();
            }
            T element = current[current.length - 1];
            DATA_HANDLE.setRelease( this, Arrays.copyOf( current, current.length - 1 ) );
            return element;
        } finally {
            lock.unlockWrite( stamp );
        }
    }

    @Override
    public boolean remove(Object element) {
        int index = indexOf( element );
        if ( index == -1 ) {
            return false;
        }
        long stamp = lock.writeLock();
        try {
            T[] current = getUnderlyingArray();
            if ( index >= current.length || element != current[index] ) {
                // contents changed, need to recheck the position of the element in the array
                int length = current.length;
                for ( index = 0; index < length; index++ ) {
                    if ( element == current[index] ) {
                        break;
                    }
                }
                if ( index == current.length ) {  // not found!
                    return false;
                }
            }
            T[] newData = Arrays.copyOf( current, current.length - 1 );
            if ( current.length - index - 1 != 0 ) {
                arraycopy( current, index + 1, newData, index, current.length - index - 1 );
            }
            DATA_HANDLE.setRelease( this, newData );
            return true;
        } finally {
            lock.unlockWrite( stamp );
        }
    }

    @Override
    public T remove(int index) {
        long stamp = lock.writeLock();
        try {
            T[] current = getUnderlyingArray();
            T old = current[index];
            T[] array = Arrays.copyOf( current, current.length - 1 );
            if ( current.length - index - 1 != 0 ) {
                arraycopy( current, index + 1, array, index, current.length - index - 1 );
            }
            DATA_HANDLE.setRelease( this, array );
            return old;
        } finally {
            lock.unlockWrite( stamp );
        }
    }

    @Override
    public void clear() {
        long stamp = lock.writeLock();
        try {
            T[] array = getUnderlyingArray();
            DATA_HANDLE.setRelease( this, Arrays.copyOf( array, 0 ) );
        } finally {
            lock.unlockWrite( stamp );
        }
    }

    @Override
    public Iterator<T> iterator() {
        T[] array = getUnderlyingArray();
        return array.length == 0 ? emptyIterator : new UncheckedIterator<>( array );
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        long stamp = lock.writeLock();
        try {
            T[] current = getUnderlyingArray();
            int oldSize = current.length;
            T[] array = Arrays.copyOf( current, oldSize + c.size() );
            for ( T element : c ) {
                array[oldSize++] = element;
            }
            DATA_HANDLE.setRelease( this, array );
            return true;
        } finally {
            lock.unlockWrite( stamp );
        }
    }

    @Override
    public boolean contains(Object o) {
        return indexOf( o ) != -1;
    }

    @Override
    public int indexOf(Object o) {
        T[] array = getUnderlyingArray();
        int length = array.length;
        for ( int i = 0; i < length; i++ ) {
            if ( o == array[i] ) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        for ( T element : this ) {
            action.accept( element );
        }
    }

    // --- //

    @Override
    public T set(int index, T element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <E> E[] toArray(E[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(int index, T element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<T> listIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<T> stream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<T> parallelStream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Spliterator<T> spliterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(Predicate<? super T> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceAll(UnaryOperator<T> operator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sort(Comparator<? super T> c) {
        throw new UnsupportedOperationException();
    }

    // --- //

    // the iterator starts from the end since elements are added to the end of the array
    private static final class UncheckedIterator<T> implements Iterator<T> {

        private final T[] snapshot;
        private int iteratorIndex;

        @SuppressWarnings( "WeakerAccess" )
        UncheckedIterator(T[] array) {
            snapshot = array;
            iteratorIndex = snapshot.length;
        }

        @Override
        public boolean hasNext() {
            return iteratorIndex > 0;
        }

        @Override
        public T next() {
            return snapshot[--iteratorIndex];
        }
    }
}
