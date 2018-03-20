package cn.bit.tao.producer.multi;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

/**
 *@author  tao wenjun
 *@date 2018��3��20��
 *Kafka�������߳�
 */

public class KafkaProducerThread implements Runnable {
    
	private static final Logger LOG = Logger.getLogger(KafkaProducerThread.class);
	
	private KafkaProducer<String, String> producer = null;
	
	private ProducerRecord<String, String> record = null;
	
	public KafkaProducerThread(KafkaProducer<String,String> producer,ProducerRecord<String,String> record){
		this.producer=producer;
		this.record=record;
	}
	/**
	 * ��д����
	 * �����̴߳����߼���ȡ���˼���
	 */
	@Override
	public void run() {
	    producer.send(record, new Callback() {
			
			@Override
			public void onCompletion(RecordMetadata metaData, Exception exception) {
				if(null!=exception){
					LOG.error("Send message occurs exception.",exception);
				}
				if(null!=metaData){
					LOG.info(String.format("offset:%s,partition:%s", metaData.offset(),metaData.partition()));
				    System.out.println(String.format("offset:%s,partition:%s", metaData.offset(),metaData.partition()));
				}
			}
		});
	}
}
