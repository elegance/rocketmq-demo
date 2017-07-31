package org.orh.rocketmq.asimple;

import java.io.UnsupportedEncodingException;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.orh.rocketmq.EnvConst;

public class OnewayProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("unique-producer-groupname-3");
        producer.setNamesrvAddr(EnvConst.NAME_SRV_ADDR);
        producer.start();

        for (int i = 0; i < 100; i++) {
            Message msg = new Message("topic-test-oneway", "tag-c", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.sendOneway(msg);
        }
        producer.shutdown();
    }
}
