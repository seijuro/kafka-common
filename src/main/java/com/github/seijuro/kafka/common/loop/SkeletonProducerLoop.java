package com.github.seijuro.kafka.common.loop;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;

/**
 * Created by myungjoonlee on 2017. 7. 4..
 */
public abstract class SkeletonProducerLoop<K, V> extends AbsLoop {
    /**
     * Instance Properties
     */
    protected final Properties props;
    protected final String topic;

    protected KafkaProducer<K, V> producer = null;

    /**
     * C'tor
     *
     * @param id
     * @param props
     */
    public SkeletonProducerLoop(int id, Properties props, String topic) {
        super(id);

        this.props = props;
        this.topic = topic;
    }

    /**
     * getter interface(s)
     * @return
     */
    protected Properties properties() { return this.props; }
    protected String topic() { return this.topic; }

    protected KafkaProducer<K, V> producer() {
        return this.producer;
    }

    @Override
    public void init() throws Exception {
    }

    @Override
    public void release() {
        if (this.producer != null) {
            this.producer.close();
            this.producer = null;
        }
    }

    @Override
    public void shutdown() {
        stop();
    }

    public Queue<ProducerRecord<K, V>> read() {
        return new LinkedList<>();
    }

    protected void send(ProducerRecord<K, V> record) {
        producer.send(record);
    }

    protected void sent(int count) {
    }

    /**
     * Runntable Interface method
     */
    public void run() {
        System.out.println("Properties : " + this.props);

        this.producer = new KafkaProducer<K, V>(this.props);

        do {
            int counnt = 0;
            Queue<ProducerRecord<K, V>> messages = read();

            assert (messages != null);

            counnt = messages.size();

            for (ProducerRecord<K, V> record = messages.poll(); record != null; record = messages.poll()) {
                send(record);
            }

            messages.clear();
            messages = null;

            sent(counnt);
        } while (isRunning());

        release();
    }
}
