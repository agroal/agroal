// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import io.agroal.api.AgroalDataSourceListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

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

    public PriorityScheduledExecutor(int executorSize, String threadPrefix, AgroalDataSourceListener... listeners) {
        super( executorSize, new PriorityExecutorThreadFactory( threadPrefix ), new ThreadPoolExecutor.CallerRunsPolicy() );
        setRemoveOnCancelPolicy( true );
        this.listeners = listeners;
    }

    @SuppressWarnings( "WeakerAccess" )
    public void executeNow(Runnable priorityTask) {
        executeNow( new PriorityTask( priorityTask ) );
    }

    @SuppressWarnings( "WeakerAccess" )
    public <T> Future<T> executeNow(Callable<T> priorityTask) {
        return executeNow( new FutureTask<>( priorityTask ) );
    }

    @SuppressWarnings( "WeakerAccess" )
    public <T> Future<T> executeNow(RunnableFuture<T> priorityFuture) {
        if ( isShutdown() ) {
            throw new RejectedExecutionException( "Task " + priorityFuture + " rejected from " + this );
        }
        priorityTasks.add( priorityFuture );
        if ( !priorityFuture.isDone() ) {
            execute( EMPTY_TASK ); // submit a dummy task so that the beforeExecute() method gets called
        }
        return priorityFuture;
    }

    @Override
    protected void beforeExecute(Thread thread, Runnable lowPriorityTask) {
        // Run all high priority tasks in queue first, then low priority
        for ( RunnableFuture<?> priorityTask; ( priorityTask = priorityTasks.poll() ) != null; ) {
            if ( isShutdown() ) {
                priorityTask.cancel( false );
            } else {
                try {
                    if ( !priorityTask.isCancelled() ) {
                        priorityTask.run();
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
        List<Runnable> allTasks = new ArrayList<>();
        for ( RunnableFuture<?> priorityTask; ( priorityTask = priorityTasks.poll() ) != null; ) {
            priorityTask.cancel( true );
            // AG-329 - hand back the task as it was submitted: a caller that inspects the returned tasks (as
            // ConnectionPool.close() does, to recover pending connection destruction) never matches the wrapper
            allTasks.add( priorityTask instanceof PriorityTask wrapper ? wrapper.unwrap() : priorityTask );
        }

        allTasks.addAll( super.shutdownNow() );
        return allTasks;
    }

    // -- //

    // Keeps a reference to the submitted task, so that it can be handed back by shutdownNow()
    private static final class PriorityTask extends FutureTask<Void> {

        private final Runnable task;

        PriorityTask(Runnable task) {
            super( task, null );
            this.task = task;
        }

        Runnable unwrap() {
            return task;
        }
    }

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
