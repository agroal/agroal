// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.wrapper;

import static java.lang.reflect.Proxy.newProxyInstance;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Statement;

import io.agroal.pool.util.AutoCloseableElement;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 * @author <a href="jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class StatementWrapper extends AbstractStatementWrapper<Statement> {

    static final String CLOSED_STATEMENT_STRING = StatementWrapper.class.getSimpleName() + ".CLOSED_STATEMENT";

    private static final InvocationHandler CLOSED_HANDLER = new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch ( method.getName() ) {
                case "close":
                    return Void.TYPE;
                case "isClosed":
                    return Boolean.TRUE;
                case "toString":
                    return CLOSED_STATEMENT_STRING;
                default:
                    throw new SQLException( "Statement is closed" );
            }
        }
    };

    private static final Statement CLOSED_STATEMENT = (Statement) newProxyInstance( Statement.class.getClassLoader(), new Class[]{Statement.class}, CLOSED_HANDLER );

    private static final AbstractStatementWrapperState<Statement> CLOSED_STATE = new AbstractStatementWrapperState<Statement>(CLOSED_STATEMENT, null);

    @Override
    protected AbstractStatementWrapperState<Statement> getClosedStatementState() {
        return CLOSED_STATE;
    }

    public StatementWrapper(ConnectionWrapper connectionWrapper, Statement statement, boolean trackResources, AutoCloseableElement head) {
        super(connectionWrapper, statement, trackResources, head);
    }

}
