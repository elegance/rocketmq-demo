package org.orh.rocketmq.border;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.orh.rocketmq.EnvConst;

public class OrderProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, MQBrokerException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("order-producer-groupname");
        producer.setNamesrvAddr(EnvConst.NAME_SRV_ADDR);
        producer.start();

        String[] tags = {"tag-a", "tag-b", "tag-c", "tag-d", "tag-e"};
        for (int i = 0; i < 100; i++) {
            int orderId = i % 10;
            Message msg = new Message("topic-test-order", tags[i % tags.length], "KEY" + i,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Integer id = (Integer) arg;
                    int index = id % mqs.size();
                    return mqs.get(index);
                }
            }, orderId);
            
            System.out.printf("%s%n", sendResult);
        }
        producer.shutdown();
    }

}
