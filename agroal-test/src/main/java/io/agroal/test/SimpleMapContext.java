// Copyright (C) 2023 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameAlreadyBoundException;
import javax.naming.NameClassPair;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class SimpleMapContext implements Context {

    private final Map<String, Object> map = new ConcurrentHashMap<>();

    @Override
    public Object lookup(Name name) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public Object lookup(String name) throws NamingException {
        return map.get( name );
    }

    @Override
    public void bind(Name name, Object obj) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public void bind(String name, Object obj) throws NamingException {
        if ( map.putIfAbsent( name, obj ) != null ) {
            throw new NameAlreadyBoundException( "Name already bound: " + name );
        }
    }

    @Override
    public void rebind(Name name, Object obj) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public void rebind(String name, Object obj) throws NamingException {
        map.put( name, obj );
    }

    @Override
    public void unbind(Name name) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public void unbind(String name) throws NamingException {
        map.remove( name );
    }

    @Override
    public void rename(Name oldName, Name newName) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public void rename(String oldName, String newName) throws NamingException {
        bind( newName, lookup( oldName ) );
        unbind( oldName );
    }

    @Override
    public NamingEnumeration<NameClassPair> list(Name name) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public NamingEnumeration<NameClassPair> list(String name) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public NamingEnumeration<Binding> listBindings(Name name) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public NamingEnumeration<Binding> listBindings(String name) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public void destroySubcontext(Name name) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public void destroySubcontext(String name) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public Context createSubcontext(Name name) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public Context createSubcontext(String name) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public Object lookupLink(Name name) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public Object lookupLink(String name) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public NameParser getNameParser(Name name) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public NameParser getNameParser(String name) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public Name composeName(Name name, Name prefix) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public String composeName(String name, String prefix) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public Object addToEnvironment(String propName, Object propVal) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public Object removeFromEnvironment(String propName) throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public Hashtable<?, ?> getEnvironment() throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    @Override
    public void close() throws NamingException {
    }

    @Override
    public String getNameInNamespace() throws NamingException {
        throw new NamingException( "Not implemented" );
    }

    public void clear() {
        map.clear();
    }
}
