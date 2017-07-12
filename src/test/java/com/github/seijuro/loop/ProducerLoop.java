package com.github.seijuro.loop;

import com.github.seijuro.kafka.common.loop.SkeletonProducerLoop;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Queue;

/**
 * Producer
 */
public class ProducerLoop extends SkeletonProducerLoop<String, String> {
    public static final int MAX_RECORDS_PER_LOOP = 1000;
    /**
     * Instance Properties
     */
    protected int lastindex = 0;

    /**
     * C'tor
     *
     * @param id
     * @param props
     * @param topic
     */
    public ProducerLoop(int id, Properties props, String topic) {
        super(id, props, topic);
    }

    public int incrementLastIndex() {
        return this.lastindex++;
    }

    @Override
    public Queue<ProducerRecord<String, String>> read() {
        Queue<ProducerRecord<String, String>> queue = super.read();

        for (int index = 0; index != MAX_RECORDS_PER_LOOP; ++index) {
            queue.add(new ProducerRecord<String, String>(this.topic(), String.format("test message : %d", incrementLastIndex())));
        }

        return queue;
    }

    @Override
    public void send(ProducerRecord<String, String> record) {
        super.send(record);

        System.out.println(String.format("Sending record is done successfully ... (record : %s)", record.toString()));
    }

    @Override
    public void sent(int count) {
        super.sent(count);

        System.out.println(String.format("The total count of message(s) : %d", count));

        try {
            if (count == 0) {
                Thread.sleep(DateUtils.MILLIS_PER_SECOND * 3);
            }
        }
        catch (InterruptedException excp) {
            excp.printStackTrace();

            stop();
        }
    }
}