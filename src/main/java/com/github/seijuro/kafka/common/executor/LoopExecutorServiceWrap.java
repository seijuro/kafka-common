package com.github.seijuro.kafka.common.executor;

import com.github.seijuro.kafka.common.loop.AbsLoop;
import org.apache.commons.lang3.time.DateUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by sogiro
 */
public class LoopExecutorServiceWrap {
    protected static final long DEFAULT_TERMINATE_WAIT_MILLIS = DateUtils.MILLIS_PER_SECOND * 5;

    protected final int poolSize;
    protected final ExecutorService executor;
    protected final List<AbsLoop> loops;
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
     * @param awaitMillis
     */
    protected LoopExecutorServiceWrap(ExecutorService service, int size, long millis) {
        this.poolSize = size > 0 ? size : 1;
        this.executor = service;
        this.awaitMillis = millis;
        this.loops = new ArrayList<>();

        this.hookShutdownThread = new Thread() {
            @Override
            public void run() {
                for (AbsLoop loop : loops) {
                    loop.shutdown();
                }

                executor.shutdown();

                try {
                    executor.awaitTermination(awaitMillis, TimeUnit.MILLISECONDS);
                } catch (InterruptedException excp) {
                    excp.printStackTrace();
                }

                for (AbsLoop loop : loops) {
                    loop.release();
                }

                loops.clear();
            }
        };

        Runtime.getRuntime().addShutdownHook(this.hookShutdownThread);

    }

    public void submit(AbsLoop task) {
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

    public void submit(AbsLoop[] tasks) {
        for (AbsLoop task : tasks) {
            submit(task);
        }
    }

    public void execute(AbsLoop task) {
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

    public void execute(AbsLoop[] tasks) {
        for (AbsLoop task : tasks) {
            this.execute(task);
        }
    }

    public void shutdown() {
        this.executor.shutdown();
    }
}
