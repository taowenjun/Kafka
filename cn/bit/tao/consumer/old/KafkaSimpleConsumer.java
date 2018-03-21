package cn.bit.tao.consumer.old;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;

import org.apache.log4j.Logger;

import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;


/**
 *@author  Tao wenjun
 *旧版Kafka Consumer
 */

public class KafkaSimpleConsumer {
    /** 日志打印对象 */
	private static final Logger LOG = Logger.getLogger(KafkaSimpleConsumer.class);
	
	/** Kafka集群代理列表 */
	private static final String BROKER_LIST = "10.108.21.2,10.108.23.236";
	
	/** 连接超时时间 */
	private static final int TIME_OUT = 60*1000;
	
	/** 读取消息缓冲区大小 */
	private static final int BUFFER_SIZE = 1024*1024;
	
	/** 每次读取消息的条数 */
	private static final int FETCH_SIZE = 100000;
	
	/** broker端口 */
	private static final int PORT = 9092;
	
	/** 容忍发生错误时重试的最大次数 */
	private static final int MAX_ERROR_NUM = 2;
	
	/**
	 * 获取分区元数据
	 * @param brokerList:服务器代理列表
	 * @param port:服务器端口
	 * @param topic:要读取消息的主题
	 * @param partitionId:要读取消息主题的分区
	 * @return 分区元数据
	 */
	private PartitionMetadata fetchPartitionMetadata(List<String> brokerList,int port,String topic,int partitionId){
		SimpleConsumer consumer = null;
		TopicMetadataRequest metadataReq = null;
		TopicMetadataResponse metadataResp = null;
		List<TopicMetadata> topicMetadata = null;
		
		try{
			for(String host:brokerList){
				//1、构造一个消费者用于获取元数据信息的执行者
				consumer = new SimpleConsumer(host, PORT, TIME_OUT, BUFFER_SIZE, "fetch-metadata");
				//2、构造请求主题的元数据的request
				metadataReq = new TopicMetadataRequest(Arrays.asList(topic));
				//3、发送获取主题元数据的请求
				metadataResp = consumer.send(metadataReq);
				//4、获取主题元数据列表
				topicMetadata = metadataResp.topicsMetadata();
				//5、主题元数据列表中提取指定分区的元数据信息
				for(TopicMetadata metadata:topicMetadata){
					for(PartitionMetadata item:metadata.partitionsMetadata()){
						if(item.partitionId()!=partitionId){
							continue;
						}else{
							return item;
						}
					}
				}
			}
		}catch(Exception e){
			LOG.error("Fetch PartitionMetadata occurs exception",e);
		}finally{
			if(null!=consumer){
				consumer.close();
			}
		}
		
		return null;	
	}
	
	/**
	 * 获取消息偏移量
	 * @param consumer:消费者
	 * @param topic:主题
	 * @param partition:分区
	 * @param beginTime:开始时间
	 * @param clientName:客户端名称
	 * @return 消息偏移量
	 */
	private long getLastOffset(SimpleConsumer consumer,String topic,int partition,long beginTime,String clientName){
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(beginTime, 1));
		OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);
		
		if(response.hasError()){
			LOG.error("Fetch last offset occurs exception:"+response.errorCode(topic, partition));
			return -1;
		}
		long[] offset=response.offsets(topic, partition);
		if(null==offset||offset.length==0){
			LOG.error("Fetch last offset occurs error ,offsets is null");
			return -1;
		}
		return offset[0];	
	}
	
	/**
	 * 消费的方法
	 * @param brokerList:服务器代理列表
	 * @param port:服务器端口
	 * @param topic:主题
	 * @param partition:分区
	 */
	public void consume(List<String > brokerList,int port,String topic,int partitionId){
		SimpleConsumer consumer = null;
		try{
			//1、首先获取指定分区的元数据信息
			PartitionMetadata metadata = fetchPartitionMetadata(brokerList, port, topic, partitionId);
		    if(metadata==null){
		    	LOG.error("Can't find metadata info");
		    	return;
		    }
		    if(metadata.leader()==null){
		    	LOG.error("Can't find the partition:"+partitionId+"'s leader.");
		    	return;
		    }
		    String leaderBroker = metadata.leader().host();
		    String clientId = "client-"+topic+"-"+partitionId;
		    
		    //2、创建一个消息者作为消费消息的真正执行者
		    consumer = new SimpleConsumer(leaderBroker,port,TIME_OUT,BUFFER_SIZE,clientId);
		    long lastOffset = getLastOffset(consumer, topic, partitionId, kafka.api.OffsetRequest.EarliestTime(), clientId);
		    int errorNum = 0;
		    kafka.api.FetchRequest fetchRequest = null;
		    FetchResponse fetchResponse = null;
		    while(lastOffset>-1){
		    	if(consumer==null){
		    		consumer=new SimpleConsumer(leaderBroker, port, TIME_OUT, BUFFER_SIZE, clientId);
		    	}
		        //3、构造获取消息的request
		        fetchRequest = new FetchRequestBuilder().clientId(clientId).addFetch(topic, partitionId, lastOffset, FETCH_SIZE).build();
		        //4、获取响应并处理
		        fetchResponse=consumer.fetch(fetchRequest);
		        if(fetchResponse.hasError()){
		        	errorNum++;
		    	    if(errorNum>MAX_ERROR_NUM){
		    	    	break;
		    	    }
		    	    
		    	    short errorCode = fetchResponse.errorCode(topic, partitionId);
		    	    if(ErrorMapping.OffsetOutOfRangeCode()==errorCode){
		    	    	lastOffset = getLastOffset(consumer, topic, partitionId, kafka.api.OffsetRequest.LatestTime(), clientId);
		    	        continue;
		    	    }else if(ErrorMapping.OffsetsLoadInProgressCode()==errorCode){
		    	    	Thread.sleep(3000);
		    	    	continue;
		    	    }else{
		    	    	consumer.close();
		    	    	consumer = null;
		    	    	continue;
		    	    }
		        }else{
		    	    errorNum = 0;
		    	    long fetchNum = 0;
		    	    for(MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partitionId)){
		    	    	long currentOffset = messageAndOffset.offset();
		    	    	if(currentOffset<lastOffset){
		    	    		LOG.error("Fetch an old offset:"+currentOffset+"expect the offset is greater than "+lastOffset);
		    	    		continue;
		    	    	}
		    	    	lastOffset = messageAndOffset.nextOffset();
		    	    	ByteBuffer payload = messageAndOffset.message().payload();
		    	    	
		    	    	byte[] bytes = new byte[payload.limit()];
		    	    	payload.get(bytes);
		    	    	//System.out.println("message:"+(new String(bytes,"UTF-8"))+",offset:"+messageAndOffset.offset());
		    	    	LOG.info("message:"+(new String(bytes,"UTF-8"))+",offset:"+messageAndOffset.offset());
		    	    	fetchNum++;
		    	    }
		    	    
		    	    if(fetchNum==0){
		    	    	try{
		    	    		Thread.sleep(1000);
		    	    	}catch(InterruptedException e){
		    	    		
		    	    	}
		    	    }
		        }
		    }
		}catch(InterruptedException | UnsupportedEncodingException e){
			LOG.error("Consume message occurs exception.",e);
		}finally{
			if(null!=consumer){
				consumer.close();
			}
		}
		
	}
	
	public static void main(String[] args){
		KafkaSimpleConsumer consumer = new KafkaSimpleConsumer();
		consumer.consume(Arrays.asList(BROKER_LIST.split(",")), PORT, "stock-quotation", 1);
	}
}
