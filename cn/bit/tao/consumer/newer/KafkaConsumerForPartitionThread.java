package cn.bit.tao.consumer.newer;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 *@author  Tao wenjun
 *新版消费者线程
 */

public class KafkaConsumerForPartitionThread extends Thread{
	//每个线程拥有私有的KafkaConsumer实例
	private KafkaConsumer<String,String> consumer;
	
	public KafkaConsumerForPartitionThread(Map<String, Object> consumerConfig,String topic){
		Properties props = new Properties();
		props.putAll(consumerConfig);
		this.consumer = new KafkaConsumer<String,String>(props);
		consumer.assign(Arrays.asList(new TopicPartition("stock-quotation", 1)));
	}
	
	@Override
	public void run(){
		try{
			while(true){
				ConsumerRecords<String, String> records = consumer.poll(1000);
				for(ConsumerRecord<String, String> record:records){
					System.out.printf("threadId=%s,partition=%d,offset=%d,key=%s value=%s\n",
							Thread.currentThread().getId(),record.partition(),record.offset(),record.key(),record.value());
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			consumer.close();
		}
	}	
}
