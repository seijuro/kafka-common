package com.github.seijuro.kafka.common.loop;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;

/**
 * Created by seijuro
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
    }

    public void prettyPrint(ConsumerRecord<K, V> record) {
        prettyPrint(String.format("meta := {topic : %s, partition : %d, offset : %d}, value := %s", record.topic(), record.partition(), record.offset(), record.value()));
    }

    public void prettyPrint(String message) {
        System.out.println(String.format("[Thread : %d] %s", this.id(), message));
    }

    /**
     * create Kafka consumer.
     *
     * @throws KafkaException
     */
    protected void createConsumer() throws KafkaException {
        final int retryMax = retryMax();

        assert (retryMax >= 1);
        int index = 1;

        do {
            try {
                this.consumer = new KafkaConsumer<K, V>(this.props);
                break;
            }
            catch (KafkaException excp) {
                if (index == retryMax) {
                    throw excp;
                }
            }
        } while (index++ < retryMax);
    }

    @Override
    public void init() {
        createConsumer();
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
