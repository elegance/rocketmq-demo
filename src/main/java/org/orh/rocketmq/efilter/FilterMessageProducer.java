package org.orh.rocketmq.efilter;

import java.io.UnsupportedEncodingException;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.orh.rocketmq.EnvConst;

public class FilterMessageProducer {
    public static void main(String[] args)
            throws MQClientException, UnsupportedEncodingException, RemotingException, MQBrokerException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("filter-producer-1");
        producer.setNamesrvAddr(EnvConst.NAME_SRV_ADDR);
        producer.start();

        for (int i = 0; i < 100; i++) {
            Message msg = new Message("filter-topic-1", "tag-a", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            
            // Here. set some properties
            msg.putUserProperty("a", String.valueOf(i));
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
//        producer.shutdown();
    }
}
