package com.github.seijuro;

import com.github.seijuro.kafka.common.executor.LoopExecutorServiceWrap;
import com.github.seijuro.loop.ProducerLoop;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by seijuro
 */
public class ProducerTestMain {
    public static final String BOOTSTRAPS = "${server1}:${port},${server2}:${port},${server3}:${port}";
    public static final String TOPIC = "${topic}";
    public static final String GROUPID = "${group}";

    public static void testProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAPS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int)(DateUtils.MILLIS_PER_SECOND * 3));

        List<ProducerLoop> loops = new ArrayList<>();

        loops.add(new ProducerLoop(0, props, TOPIC));
        loops.add(new ProducerLoop(1, props, TOPIC));
        loops.add(new ProducerLoop(2, props, TOPIC));
        loops.add(new ProducerLoop(3, props, TOPIC));

        LoopExecutorServiceWrap executors = LoopExecutorServiceWrap.newFixedThreadPool(4);

        for (ProducerLoop loop : loops) {
            executors.submit(loop);
        }
    }

    public static void main(String[] args) {
        testProducer();
    }
}
