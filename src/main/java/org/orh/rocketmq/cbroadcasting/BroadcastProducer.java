package org.orh.rocketmq.cbroadcasting;

import java.io.UnsupportedEncodingException;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.orh.rocketmq.EnvConst;

public class BroadcastProducer {
    public static void main(String[] args)
            throws UnsupportedEncodingException, MQClientException, RemotingException, MQBrokerException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("broadcast-group");
        producer.setNamesrvAddr(EnvConst.NAME_SRV_ADDR);
        producer.start();

        for (int i = 0; i < 100; i++) {
            Message msg = new Message("broadcast-topic-1", "tag-a", "order-id-188",
                    ("Hello RocketMQ - broadcast" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        producer.shutdown();
    }
}
