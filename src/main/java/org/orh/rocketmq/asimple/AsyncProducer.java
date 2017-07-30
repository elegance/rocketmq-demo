package org.orh.rocketmq.asimple;

import java.io.UnsupportedEncodingException;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class AsyncProducer {
	public static void main(String[] args)
			throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException {
		DefaultMQProducer producer = new DefaultMQProducer("unique-producer-groupname-2");
		producer.setNamesrvAddr("127.0.0.1:9876");

		// launch producer
		producer.start();
		producer.setRetryTimesWhenSendAsyncFailed(0);

		for (int i = 0; i < 100; i++) {
			final int index = i;

			Message msg = new Message("topic-test-async", "tag-b", "order-id-188",
					"HelloWorld RocketMQ".getBytes(RemotingHelper.DEFAULT_CHARSET));
			producer.send(msg, new SendCallback() {

				@Override
				public void onSuccess(SendResult sendResult) {
					System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
				}

				@Override
				public void onException(Throwable e) {
					System.out.printf("%-10d Exception %s %n", index, e);

				}
			});
		}
		
//		producer.shutdown();

	}
}
