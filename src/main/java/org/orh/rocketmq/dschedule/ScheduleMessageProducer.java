package org.orh.rocketmq.dschedule;

import java.io.UnsupportedEncodingException;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.orh.rocketmq.EnvConst;

/**
 * 2. Send scheduled messages
 *
 */
public class ScheduleMessageProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, MQBrokerException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("schedule-producer-1");
        producer.setNamesrvAddr(EnvConst.NAME_SRV_ADDR);

        producer.start();
        int totalMessagesToSend = 100;

        for (int i = 0; i < totalMessagesToSend; i++) {
            Message msg = new Message("test-topic-schedule", ("Hello scheduled message " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // this message will be delivered to consumer 10 seconds later.
            msg.setDelayTimeLevel(3);
            producer.send(msg);
        }
//        producer.shutdown();
    }
}
