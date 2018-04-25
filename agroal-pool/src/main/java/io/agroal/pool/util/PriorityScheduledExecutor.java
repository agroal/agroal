// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class PriorityScheduledExecutor extends ScheduledThreadPoolExecutor {

    private static final AtomicLong THREAD_COUNT = new AtomicLong();
    private static final Runnable EMPTY_TASK = () -> {
    };

    private final Queue<RunnableFuture<?>> priorityTasks = new ConcurrentLinkedQueue<>();

    public PriorityScheduledExecutor(int executorSize, String threadPrefix) {
        super( executorSize, r -> {
            Thread thread = new Thread( r, threadPrefix + THREAD_COUNT.incrementAndGet() );
            thread.setDaemon( true );
            return thread;
        } );
    }

    public <T> Future<T> executeNow(Runnable priorityTask) {
        RunnableFuture<T> priorityFuture = new FutureTask<>( priorityTask, null );
        priorityTasks.add( priorityFuture );
        // Submit a task so that the beforeExecute() method gets called
        execute( EMPTY_TASK );
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
        executeNow( super::shutdown );
    }

    @Override
    public List<Runnable> shutdownNow() {
        for ( RunnableFuture<?> runnableFuture : priorityTasks ) {
            runnableFuture.cancel( true );
        }
        priorityTasks.clear();
        return super.shutdownNow();
    }
}
