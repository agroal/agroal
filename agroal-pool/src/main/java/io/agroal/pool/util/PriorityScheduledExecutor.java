// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

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

    public PriorityScheduledExecutor(int executorSize, String threadPrefix) {
        super( executorSize, new PriorityExecutorThreadFactory( threadPrefix ) );
    }

    public void executeNow(Runnable priorityTask) {
        executeNow( new FutureTask<>( priorityTask, null ) );
    }

    public <T> Future<T> executeNow(Callable<T> priorityTask) {
        return executeNow( new FutureTask<>( priorityTask ) );
    }

    public <T> Future<T> executeNow(RunnableFuture<T> priorityFuture) {
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
            } else {
                priorityTask.run();
            }
        }
        super.beforeExecute( thread, lowPriorityTask );
    }

    @Override
    public void shutdown() {
        if ( !isShutdown() ) {
            executeNow( super::shutdown );
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        for ( RunnableFuture<?> runnableFuture : priorityTasks ) {
            runnableFuture.cancel( true );
        }
        priorityTasks.clear();
        return super.shutdownNow();
    }

    // -- //

    private static class PriorityExecutorThreadFactory implements ThreadFactory {

        private final AtomicLong threadCount;
        private final String threadPrefix;

        public PriorityExecutorThreadFactory(String threadPrefix) {
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
