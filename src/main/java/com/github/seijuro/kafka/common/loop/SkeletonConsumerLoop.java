package com.github.seijuro.kafka.common.loop;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;

/**
 * Created by sogiro
 */
public abstract class SkeletonConsumerLoop<K, V>  extends AbsLoop {
    /**
     * Instance Properties
     */
    protected List<String> topics;
    protected KafkaConsumer<K, V> consumer;
    protected Properties props;

    /**
     * C'tor
     *
     * @param id
     * @param topics\
     */
    public SkeletonConsumerLoop(
            int id,
            Properties props,
            List<String> topics) {
        super(id);

        this.props = props;
        this.topics = topics;

        System.out.println(String.format("id : %d, props : %s, topics :%s", id, props, topics));

        this.consumer = new KafkaConsumer(this.props);
    }

    public void prettyPrint(ConsumerRecord<K, V> record) {
        prettyPrint(String.format("meta := {topic : %s, partition : %d, offset : %d}, value := %s", record.topic(), record.partition(), record.offset(), record.value()));
    }

    public void prettyPrint(String message) {
        System.out.println(String.format("[Thread : %d] %s", this.id(), message));
    }

    @Override
    public void init() {
    }

    protected void process(ConsumerRecords<K, V> records) {
    }

    @Override
    public void release() {
        super.release();
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    @Override
    public void run() {
        try {
            this.consumer.subscribe(this.topics);

            do {
                ConsumerRecords<K, V> records = consumer.poll(Long.MAX_VALUE);

                //  handle records result as polling ...
                process(records);

            } while (this.running);
        }
        catch (WakeupException excp) {
            excp.printStackTrace();

            this.running = false;
        }

        release();
    }
}
