package com.github.seijuro.kafka.common.executor;

import com.github.seijuro.kafka.common.loop.AbstractLoop;
import org.apache.commons.lang3.time.DateUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by seijuro
 */
public class LoopExecutorServiceWrap {
    protected static final long DEFAULT_TERMINATE_WAIT_MILLIS = DateUtils.MILLIS_PER_MINUTE;

    protected final int poolSize;
    protected final ExecutorService executor;
    protected final List<AbstractLoop> loops;
    protected final Thread hookShutdownThread;
    protected final long awaitMillis;

    public static LoopExecutorServiceWrap newFixedThreadPool(int size) {
        ExecutorService executor = Executors.newFixedThreadPool(size);

        return new LoopExecutorServiceWrap(executor, size);
    }

    /**
     * C'tor
     *
     * @param service
     * @param size
     */
    protected LoopExecutorServiceWrap(ExecutorService service, int size) {
        this(service, size, DEFAULT_TERMINATE_WAIT_MILLIS);
    }

    /**
     * C'tor
     *
     * @param service
     * @param size
     * @param millis
     */
    protected LoopExecutorServiceWrap(ExecutorService service, int size, long millis) {
        this.poolSize = size > 0 ? size : 1;
        this.executor = service;
        this.awaitMillis = millis;
        this.loops = new ArrayList<>();

        this.hookShutdownThread = new Thread() {
            @Override
            public void run() {
                for (AbstractLoop loop : loops) {
                    loop.shutdown();
                }

                executor.shutdown();

                try {
                    executor.awaitTermination(awaitMillis, TimeUnit.MILLISECONDS);
                } catch (InterruptedException excp) {
                    excp.printStackTrace();
                }

                loops.clear();
            }
        };

        Runtime.getRuntime().addShutdownHook(this.hookShutdownThread);
    }

    public void submit(AbstractLoop task) {
        try {
            task.init();
            this.executor.submit(task);
            this.loops.add(task);

            System.out.println(String.format("submit loop done ... (id : %d)", task.id()));
        }
        catch (Exception excp) {
            excp.printStackTrace();
        }
    }

    public void submit(AbstractLoop[] tasks) {
        for (AbstractLoop task : tasks) {
            submit(task);
        }
    }

    public void execute(AbstractLoop task) {
        try {
            task.init();
            this.executor.execute(task);
            this.loops.add(task);

            System.out.println(String.format("execute loop done ... (id : %d)", task.id()));
        }
        catch (Exception excp) {
            excp.printStackTrace();
        }
    }

    public void execute(AbstractLoop[] tasks) {
        for (AbstractLoop task : tasks) {
            this.execute(task);
        }
    }

    public void shutdown() {
        this.executor.shutdown();
    }
}
