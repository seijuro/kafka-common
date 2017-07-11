package com.github.sogiro.kafka.common.executor;

import com.github.sogiro.kafka.common.loop.ConsumerLoop;
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
    protected static final long DEFAULT_TERMINATE_WAIT_MILLIS = DateUtils.MILLIS_PER_SECOND * 3;

    protected final int poolSize;
    protected final ExecutorService executor;
    protected final List<ConsumerLoop> loops;
    protected final Thread hookShutdownThread;
    protected final long awaitMillis = DEFAULT_TERMINATE_WAIT_MILLIS;

    public static LoopExecutorServiceWrap newFixedThreadPool(int size) {
        ExecutorService executor = Executors.newFixedThreadPool(size);

        return new LoopExecutorServiceWrap(executor, size);
    }

    /**
     * C'tor
     *
     * @param size
     */
    protected LoopExecutorServiceWrap(ExecutorService service, int size) {
        this.poolSize = size > 0 ? size : 1;
        this.executor = service;
        this.loops = new ArrayList<>();
        this.hookShutdownThread = new Thread() {
            @Override
            public void run() {
                for (ConsumerLoop loop : loops) {
                    loop.shudown();
                }

                executor.shutdown();

                try {
                    executor.awaitTermination(awaitMillis, TimeUnit.MILLISECONDS);
                } catch (InterruptedException excp) {
                    excp.printStackTrace();
                }

                for (ConsumerLoop loop : loops) {
                    loop.release();
                }

                loops.clear();
            }
        };

        Runtime.getRuntime().addShutdownHook(this.hookShutdownThread);
    }

    public void submit(ConsumerLoop task) {
        task.init();
        this.loops.add(task);

        this.executor.submit(task);
    }

    public void submit(ConsumerLoop[] tasks) {
        for (ConsumerLoop task : tasks) {
            submit(task);
        }
    }

    public void execute(ConsumerLoop[] tasks) {
        for (ConsumerLoop task : tasks) {
            task.init();
            this.loops.add(task);

            this.executor.execute(task);
        }
    }
}
