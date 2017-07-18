package com.github.seijuro.kafka.common.loop;

/**
 * Created by seijuro
 */
public abstract class AbsLoop implements Runnable {
    public static final int DEFAULT_RETRY_MAX = 3;

    /**
     * Instance Properties
     */
    protected boolean running = true;
    private final int id;
    private final int retry;

    public AbsLoop(int $id, int $retry) {
        this.id = $id;
        this.retry = $retry;
    }

    public AbsLoop(int $id) {
        this($id, DEFAULT_RETRY_MAX);
    }

    public int id() {
        return this.id;
    }

    public int retryMax() {
        return this.retry;
    }

    public boolean isRunning() {
        return this.running;
    }

    public void stop() {
        this.running = false;
    }

    public void init() throws Exception {
    }

    public void release() {
    }

    public void shutdown() {
        stop();
    }

    @Override
    public void run() {
    }
}
