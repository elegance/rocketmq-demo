package org.orh.rocketmq.border;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.orh.rocketmq.EnvConst;

public class OrderdConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order-producer-groupname");
        consumer.setNamesrvAddr(EnvConst.NAME_SRV_ADDR);

        consumer.subscribe("topic-test-order", "tag-a || tag-c || tag-d");

        consumer.registerMessageListener(new MessageListenerOrderly() {
            AtomicLong consumerTimes = new AtomicLong(0);

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                context.setAutoCommit(false);
                System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msgs + "%n");
                this.consumerTimes.incrementAndGet();
                if (this.consumerTimes.get() % 2 == 0) {
                    return ConsumeOrderlyStatus.SUCCESS;
                } else if (this.consumerTimes.get() % 3 == 0) {
                    return ConsumeOrderlyStatus.ROLLBACK;
                } else if (this.consumerTimes.get() % 4 == 0) {
                    return ConsumeOrderlyStatus.COMMIT;
                } else if (this.consumerTimes.get() % 5 == 0) {
                    context.setSuspendCurrentQueueTimeMillis(3000);
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
        System.out.print("Consumer Started.%n");
    }
}
