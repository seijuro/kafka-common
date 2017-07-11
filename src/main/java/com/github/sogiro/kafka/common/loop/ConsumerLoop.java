package com.github.sogiro.kafka.common.loop;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;

/**
 * Created by sogiro
 */
public class ConsumerLoop<K, V>  implements Runnable {
    /**
     * Instance Properties
     */
    protected boolean running = true;

    protected int threadId;
    protected List<String> topics;
    protected KafkaConsumer<K, V> consumer;
    protected Properties props;

    /**
     * C'tor
     *
     * @param id
     * @param topics\
     */
    public ConsumerLoop(
            int id,
            Properties props,
            List<String> topics) {
        this.threadId = id;
        this.props = props;
        this.topics = topics;

        System.out.println(String.format("id : %d, props : %s, topics :%s", id, props, topics));

        this.consumer = new KafkaConsumer(this.props);
    }

    public void prettyPrint(ConsumerRecord<K, V> record) {
        prettyPrint(String.format("meta := {topic : %s, partition : %d, offset : %d}, value := %s", record.topic(), record.partition(), record.offset(), record.value()));
    }

    public void prettyPrint(String message) {
        System.out.println(String.format("[Thread : %d] %s", this.threadId, message));
    }

    public void init() {
    }

    protected void poll(ConsumerRecords<K, V> records) {
    }

    public void release() {
    }

    public void shudown() {
        this.running = false;
    }

    public void run() {
        try {
            this.consumer.subscribe(this.topics);

            do {
                ConsumerRecords<K, V> records = consumer.poll(Long.MAX_VALUE);

                //  handle records result as polling ...
                poll(records);

            } while (this.running);
        }
        catch (WakeupException excp) {
            excp.printStackTrace();

            this.running = false;
        }

        release();
    }
}
