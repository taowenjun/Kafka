package cn.bit.tao.consumer.newer;

import java.util.HashMap;
import java.util.Map;

/**
 *@author  tao wenjun
 *新版Kafka消费者
 */

public class KafkaConsumer {
	private static final String BROKER_LISR="master:9092,slave1:9092,slave2:9092";
	
	private static final String STRING_DES="org.apache.kafka.common.serialization.StringDeserializer";
	
	public static void main(String[] args) {
		Map<String, Object> config = new HashMap<String,Object>();
		config.put("bootstrap.servers", BROKER_LISR);
		config.put("group.id", "test-consumer-group");
		config.put("client.id","test");
		config.put("enable.auto.commit", true);
		config.put("auto.commit.interval.ms", 1000);
		config.put("key.deserializer", STRING_DES);
		config.put("value.deserializer", STRING_DES);
		
		for(int i=0;i<1;i++){
			new KafkaConsumerThread(config, "stock-quotation").start();
		}
	}

}
