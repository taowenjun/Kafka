package cn.bit.tao.partitioner;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.log4j.Logger;

/**
 *@author  Tao wenjun
 *自定义分区器
 */

public class StockPartitioner implements Partitioner {
	private static final Logger LOG = Logger.getLogger(StockPartitioner.class);
	
	private static final Integer PARTITION = 6;
	
	@Override
	public void configure(Map<String, ?> configs) {
		
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		if(null==key){
			return 0;
		}
		String stockCode = String.valueOf(key);
		try{
			int partitionId = Integer.valueOf(stockCode.substring(stockCode.length()-2))%PARTITION;
			return partitionId;
		}catch(NumberFormatException e){
			LOG.error("Parse message key occurs exception,key:"+stockCode,e);
		    return 0;
		}
		
	}

	@Override
	public void close() {
		
	}

}
