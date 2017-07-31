package org.orh.rocketmq.asimple;

import java.io.UnsupportedEncodingException;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.orh.rocketmq.EnvConst;

public class SyncProducer {
	public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, MQBrokerException, InterruptedException {
		// Instantiate with a producer group name
		DefaultMQProducer producer = new DefaultMQProducer("unique_producer_groupname_1");
		producer.setNamesrvAddr(EnvConst.NAME_SRV_ADDR);
		
		// launch producer
		producer.start();
		
		for (int i = 0; i < 100; i++) {
			Message msg = new Message("topic-test", "Tag-A", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
			SendResult sendResult = producer.send(msg);
			System.out.printf("%s%n", sendResult);
			
		}
		
		// Shut down once the producer instance is not longer in use. 
		producer.shutdown();
	}
}
