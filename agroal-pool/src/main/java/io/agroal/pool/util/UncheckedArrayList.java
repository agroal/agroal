// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.lang.System.arraycopy;
import static java.lang.reflect.Array.newInstance;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class UncheckedArrayList<T> implements List<T> {

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

    private T[] data;
    private int size;

    @SuppressWarnings( "unchecked" )
    private UncheckedArrayList(Class<?> clazz, int capacity) {
        this.data = (T[]) newInstance( clazz, capacity );
        this.size = 0;
    }

    public UncheckedArrayList(Class<?> clazz) {
        this( clazz, 4 );
    }

    public UncheckedArrayList(Class<?> clazz, T[] initial) {
        this( clazz, initial.length );
        this.size = initial.length;
        arraycopy( initial, 0, data, 0, size );
    }

    @Override
    public boolean add(T element) {
        if ( size >= data.length ) {
            data = Arrays.copyOf( data, data.length << 1 );
        }
        data[size] = element;
        size++;
        return true;
    }

    @Override
    public T get(int index) {
        return data[index];
    }

    @Override
    public boolean remove(Object element) {
        for ( int index = size - 1; index >= 0; index-- ) {
            if ( element == data[index] ) {
                int numMoved = size - index - 1;
                if ( numMoved > 0 ) {
                    arraycopy( data, index + 1, data, index, numMoved );
                }
                data[--size] = null;
                return true;
            }
        }
        return false;
    }

    @Override
    public void clear() {
        for ( int i = 0; i < size; i++ ) {
            data[i] = null;
        }
        size = 0;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public T set(int index, T element) {
        T old = data[index];
        data[index] = element;
        return old;
    }

    @Override
    public T remove(int index) {
        if ( size == 0 ) {
            return null;
        }
        T old = data[index];
        int moved = size - index - 1;
        if ( moved > 0 ) {
            arraycopy( data, index + 1, data, index, moved );
        }
        data[--size] = null;
        return old;
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
        return size == 0 ? emptyIterator : new UncheckedIterator<>( data );
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
    public boolean addAll(Collection<? extends T> c) {
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
    public int indexOf(Object o) {
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
    public void forEach(Consumer<? super T> action) {
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

    private static final class UncheckedIterator<T> implements Iterator<T> {

        private final int size;

        private final T[] data;

        private int index = 0;

        public UncheckedIterator(T[] data) {
            this.data = data;
            this.size = data.length;
        }

        @Override
        public boolean hasNext() {
            return index < size;
        }

        @Override
        public T next() {
            if ( index < size ) {
                return data[index++];
            }
            throw new NoSuchElementException( "No more elements in this list" );
        }
    }
}
