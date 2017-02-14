// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool.util;

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
public class PriorityScheduledExecutor extends ScheduledThreadPoolExecutor {

    private static final AtomicLong THREAD_COUNT = new AtomicLong();

    private Queue<RunnableFuture<?>> highPriorityTasks = new ConcurrentLinkedQueue<>();

    public PriorityScheduledExecutor(int executorSize, String threadPrefix) {
        super( executorSize, r -> {
            Thread thread = new Thread( r, threadPrefix + THREAD_COUNT.incrementAndGet() );
            thread.setDaemon( true );
            return thread;
        } );
    }

    public Future<?> executeNow(Runnable priorityTask) {
        RunnableFuture<?> priorityFuture = new FutureTask<>( priorityTask, null );
        highPriorityTasks.add( priorityFuture );
        submit( () -> {} );
        return priorityFuture;
    }

    @Override
    protected void beforeExecute(Thread thread, Runnable lowPriorityTask) {
        // Run all high priority tasks in queue first, then low priority
        RunnableFuture<?> priorityTask;
        while ( ( priorityTask = highPriorityTasks.poll() ) != null ) {
            priorityTask.run();
        }
        super.beforeExecute( thread, lowPriorityTask );
    }
}
