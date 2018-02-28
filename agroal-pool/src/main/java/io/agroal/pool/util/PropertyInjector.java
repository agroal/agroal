// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Locale;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class PropertyInjector {

    public static void inject(Object target, String propertyName, Object propertyValue) {
        try {
            injectMethod( target, propertyName, propertyValue );
        } catch ( NoSuchMethodException | IllegalAccessException me ) {
            try {
                injectField( target, propertyName, propertyValue );
            } catch ( NoSuchFieldException | IllegalAccessException fe ) {
                try {
                    injectSetProperty( target, propertyName, propertyValue );
                } catch ( NoSuchMethodException | IllegalAccessException e ) {
                    throw new IllegalArgumentException( "Unable to inject property " + propertyName + " of type " + propertyValue.getClass() + " into " + target.getClass() );
                } catch ( InvocationTargetException e ) {
                    throw new IllegalArgumentException( "Unable to inject property " + propertyName, e.getTargetException() );
                }
            } catch ( InvocationTargetException e ) {
                throw new IllegalArgumentException( "Unable to inject property " + propertyName, e.getTargetException() );
            }
        } catch ( InvocationTargetException e ) {
            throw new IllegalArgumentException( "Unable to inject property " + propertyName, e.getTargetException() );
        }
    }

    // --- //

    private static void injectMethod(Object target, String propertyName, Object propertyValue) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        try {
            injectPublicMethod( target, propertyName, propertyValue );
        } catch ( NoSuchMethodException | IllegalAccessException | InvocationTargetException me ) {
            injectPrivateMethod( target, propertyName, propertyValue );
        }
    }

    private static void injectPublicMethod(Object target, String propertyName, Object propertyValue) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = target.getClass().getMethod( methodName( propertyName ), propertyValue.getClass() );
        method.invoke( target, propertyValue );
    }

    private static void injectPrivateMethod(Object target, String propertyName, Object propertyValue) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = target.getClass().getDeclaredMethod( methodName( propertyName ), propertyValue.getClass() );
        method.setAccessible( true );
        method.invoke( target, propertyValue );
    }

    private static String methodName(String propertyName) {
        StringBuilder methodName = new StringBuilder( "set" ).append( propertyName.substring( 0, 1 ).toUpperCase( Locale.US ) );
        if ( propertyName.length() > 1 ) {
            methodName.append( propertyName.substring( 1 ) );
        }
        return methodName.toString();
    }

    // --- //

    private static void injectField(Object target, String propertyName, Object propertyValue) throws NoSuchFieldException, InvocationTargetException, IllegalAccessException {
        try {
            injectPublicField( target, propertyName, propertyValue );
        } catch ( NoSuchFieldException | IllegalAccessException | InvocationTargetException me ) {
            injectPrivateField( target, propertyName, propertyValue );
        }
    }

    private static void injectPublicField(Object target, String propertyName, Object propertyValue) throws NoSuchFieldException, InvocationTargetException, IllegalAccessException {
        Field field = target.getClass().getField( propertyName );
        field.set( target, propertyValue );
    }

    private static void injectPrivateField(Object target, String propertyName, Object propertyValue) throws NoSuchFieldException, InvocationTargetException, IllegalAccessException {
        Field field = target.getClass().getDeclaredField( propertyName );
        field.set( target, propertyValue );
    }

    // --- //

    private static void injectSetProperty(Object target, String propertyName, Object propertyValue) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = target.getClass().getMethod( methodName( "property" ), propertyName.getClass(), propertyValue.getClass() );
        method.invoke( target, propertyName, propertyValue );
    }
}
