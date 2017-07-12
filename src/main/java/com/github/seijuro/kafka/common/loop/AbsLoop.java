package com.github.seijuro.kafka.common.loop;

/**
 * Created by seijuro
 */
public abstract class AbsLoop implements Runnable {
    /**
     * Instance Properties
     */
    protected boolean running = true;
    private final int id;

    public AbsLoop(int id) {
        this.id = id;
    }

    public int id() {
        return this.id;
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
