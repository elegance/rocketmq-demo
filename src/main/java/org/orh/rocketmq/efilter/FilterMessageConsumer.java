package org.orh.rocketmq.efilter;

import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.orh.rocketmq.EnvConst;

public class FilterMessageConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("filter-consumer-1");
        consumer.setNamesrvAddr(EnvConst.NAME_SRV_ADDR);

        // Here. According to properties to select
        consumer.subscribe("filter-topic-1", MessageSelector.bySql("a between 0 and 3"));
//        consumer.subscribe("filter-topic-1", MessageSelector.byTag("tag-a"));


        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                msgs.stream().forEach(msg -> {
                    System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msg + ", Property a: " + msg.getProperty("a") + "%n");
                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }
}
