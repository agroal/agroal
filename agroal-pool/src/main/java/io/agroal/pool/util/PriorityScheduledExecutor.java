// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import io.agroal.api.AgroalDataSourceListener;
import io.agroal.pool.ConnectionHandler;
import io.agroal.pool.CreateConnectionFuture;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import static io.agroal.pool.util.ListenerHelper.fireOnConnectionCreationTimeout;
import static io.agroal.pool.util.ListenerHelper.fireOnWarning;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class PriorityScheduledExecutor extends ScheduledThreadPoolExecutor {

    private static final Runnable EMPTY_TASK = new Runnable() {
        @Override
        public void run() {
        }
    };

    private final Queue<RunnableFuture<?>> priorityTasks = new ConcurrentLinkedQueue<>();
    private final AgroalDataSourceListener[] listeners;
    private final ExecutorService connectionCreationExecutor;
    private final Duration connectionCreationTimeout;

    public PriorityScheduledExecutor(int executorSize, String threadPrefix, Duration connectionCreationTimeout, AgroalDataSourceListener... listeners) {
        super( executorSize, new PriorityExecutorThreadFactory( threadPrefix ), new ThreadPoolExecutor.CallerRunsPolicy() );
        setRemoveOnCancelPolicy( true );
        this.listeners = listeners;
        this.connectionCreationTimeout = connectionCreationTimeout;
        if(connectionCreationTimeout.isZero()) {
            this.connectionCreationExecutor = null;
        }else {
            this.connectionCreationExecutor = Executors.newCachedThreadPool(this.getThreadFactory());
        }
    }

    @SuppressWarnings( "WeakerAccess" )
    private void executeNow(Runnable priorityTask) {
        executeNow( new FutureTask<>( priorityTask, null ) );
    }

    @SuppressWarnings( "WeakerAccess" )
    public Future<ConnectionHandler> executeNow(Callable<ConnectionHandler> priorityTask) {
        return executeNow(new CreateConnectionFuture(priorityTask));
    }

    @SuppressWarnings( "WeakerAccess" )
    private <T> Future<T> executeNow(RunnableFuture<T> priorityFuture) {
        if ( isShutdown() ) {
            throw new RejectedExecutionException( "Task " + priorityFuture + " rejected from " + this );
        }
        priorityTasks.add( priorityFuture );
        if ( !priorityFuture.isDone() ) {
            // Submit a task so that the beforeExecute() method gets called
            execute( EMPTY_TASK );
        }
        return priorityFuture;
    }

    @Override
    protected void beforeExecute(Thread thread, Runnable lowPriorityTask) {
        // Run all high priority tasks in queue first, then low priority
        for ( RunnableFuture<?> priorityTask; ( priorityTask = priorityTasks.poll() ) != null; ) {
            if ( isShutdown() ) {
                priorityTask.cancel( false );
                fireOnConnectionCreationTimeout(listeners);
            } else {
                try {
                    if(this.connectionCreationExecutor == null || !(priorityTask instanceof CreateConnectionFuture)) {
                        // Do not use another thread in case there is anyhow no timeout!
                        // Do not use another thread if it is not a connection creation!
                        priorityTask.run();
                    }else{
                        Future<?> future = this.connectionCreationExecutor.submit(priorityTask);
                        try{
                            future.get(connectionCreationTimeout.toNanos(), TimeUnit.NANOSECONDS);
                        }catch (TimeoutException t){
                            priorityTask.cancel( true );
                            future.cancel( true );
                            fireOnConnectionCreationTimeout(listeners);
                        }
                    }
                    afterExecute( priorityTask, null );
                } catch ( Throwable t ) {
                    afterExecute( priorityTask, t );
                }
            }
        }
        super.beforeExecute( thread, lowPriorityTask );
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        if ( t != null ) {
            fireOnWarning( listeners, t );
        }
        super.afterExecute( r, t );
    }

    @Override
    public void shutdown() {
        if ( !isShutdown() ) {
            executeNow( super::shutdown );
            execute( this::shutdownNow );
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        for ( RunnableFuture<?> runnableFuture : priorityTasks ) {
            runnableFuture.cancel( true );
            fireOnConnectionCreationTimeout(listeners);
        }

        if(connectionCreationExecutor != null){
            this.connectionCreationExecutor.shutdown();
        }

        List<Runnable> allTasks = new ArrayList<>( priorityTasks );
        priorityTasks.clear();

        allTasks.addAll( super.shutdownNow() );
        return allTasks;
    }

    // -- //

    private static class PriorityExecutorThreadFactory implements ThreadFactory {

        private final AtomicLong threadCount;
        private final String threadPrefix;

        @SuppressWarnings( "WeakerAccess" )
        PriorityExecutorThreadFactory(String threadPrefix) {
            this.threadPrefix = threadPrefix;
            threadCount = new AtomicLong();
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread( r, threadPrefix + threadCount.incrementAndGet() );
            thread.setDaemon( true );
            return thread;
        }
    }
}
