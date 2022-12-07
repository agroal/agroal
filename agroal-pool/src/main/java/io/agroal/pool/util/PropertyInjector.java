// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class PropertyInjector {

    private final Class<?> cls;

    private final Map<String, Method> propertySetter = new HashMap<>();

    @SuppressWarnings( "CallToSuspiciousStringMethod" )
    public PropertyInjector(Class<?> cls) {
        this.cls = cls;

        for ( Method method : cls.getMethods() ) {
            String name = method.getName();
            if ( method.getParameterCount() == 1 && name.startsWith( "set" ) ) {
                propertySetter.merge( name.substring( 3 ), method, PropertyInjector::methodSelector );
            } else if ( method.getParameterCount() == 2 && "setProperty".equals( name ) ) {
                propertySetter.put( "Property", method );
            }
        }
    }

    /**
     * Method to resolve ambiguities when multiple methods apply the same property.
     */
    private static Method methodSelector(Method methodOne, Method methodTwo) {
        // AG-200 - Prefer methods that are not deprecated
        return methodOne.isAnnotationPresent( Deprecated.class ) ? methodTwo : methodOne;
    }

    @SuppressWarnings( "StringConcatenation" )
    public void inject(Object target, String propertyName, String propertyValue) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        String realName = propertyName.substring( 0, 1 ).toUpperCase( Locale.ROOT ) + propertyName.substring( 1 );
        if ( propertySetter.containsKey( realName ) ) {
            Method method = propertySetter.get( realName );
            method.invoke( target, typeConvert( propertyValue, method.getParameterTypes()[0] ) );
        } else if ( propertySetter.containsKey( "Property" ) ) {
            cls.getMethod( "setProperty", propertyName.getClass(), propertyValue.getClass() ).invoke( target, propertyName, propertyValue );
        } else {
            throw new NoSuchMethodException( "No setter in class " + cls.getName() );
        }
    }

    public Set<String> availableProperties() {
        return propertySetter.keySet();
    }

    @SuppressWarnings( "CallToSuspiciousStringMethod" )
    private static Properties typeConvertProperties(String properties) {
        if ( properties == null ) {
            return new Properties();
        }
        properties = properties.trim();
        if ( properties.isEmpty() ) {
            return new Properties();
        }
        Properties result = new Properties();
        for ( String property : properties.split( ";" ) ) {
            String[] keyValue = property.split( "=" );
            if ( keyValue.length != 2 ) {
                throw new IllegalArgumentException( "Can't convert properties '" + properties + "' to " + Properties.class.getName() );
            }
            result.put( keyValue[0].trim(), keyValue[1].trim() );
        }
        return result;
    }

    private Object typeConvert(String value, Class<?> target) {
        if ( target == String.class ) {
            return value;
        } else if ( target == byte[].class ) {
            return value.getBytes( StandardCharsets.UTF_8 );
        } else if ( target == char[].class ) {
            return value.toCharArray();
        } else if ( Boolean.class.isAssignableFrom( target ) || Boolean.TYPE.isAssignableFrom( target ) ) {
            return Boolean.parseBoolean( value );
        } else if ( Character.class.isAssignableFrom( target ) || Character.TYPE.isAssignableFrom( target ) ) {
            return value.charAt( 0 );
        } else if ( Short.class.isAssignableFrom( target ) || Short.TYPE.isAssignableFrom( target ) ) {
            return Short.parseShort( value );
        } else if ( Integer.class.isAssignableFrom( target ) || Integer.TYPE.isAssignableFrom( target ) ) {
            return Integer.parseInt( value );
        } else if ( Long.class.isAssignableFrom( target ) || Long.TYPE.isAssignableFrom( target ) ) {
            return Long.parseLong( value );
        } else if ( Float.class.isAssignableFrom( target ) || Float.TYPE.isAssignableFrom( target ) ) {
            return Float.parseFloat( value );
        } else if ( Double.class.isAssignableFrom( target ) || Double.TYPE.isAssignableFrom( target ) ) {
            return Double.parseDouble( value );
        } else if ( Class.class.isAssignableFrom( target ) ) {
            try {
                return cls.getClassLoader().loadClass( value );
            } catch ( ClassNotFoundException e ) {
                throw new IllegalArgumentException( "ClassNotFoundException: " + e.getMessage() );
            }
        } else if ( Properties.class.isAssignableFrom( target ) ) {
            return typeConvertProperties( value );
        } else {
            throw new IllegalArgumentException( "Can't convert to " + target.getName() );
        }
    }
}
