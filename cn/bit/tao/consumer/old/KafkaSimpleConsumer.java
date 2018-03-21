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
 *�ɰ�Kafka Consumer
 */

public class KafkaSimpleConsumer {
    /** ��־��ӡ���� */
	private static final Logger LOG = Logger.getLogger(KafkaSimpleConsumer.class);
	
	/** Kafka��Ⱥ�����б� */
	private static final String BROKER_LIST = "10.108.21.2,10.108.23.236";
	
	/** ���ӳ�ʱʱ�� */
	private static final int TIME_OUT = 60*1000;
	
	/** ��ȡ��Ϣ��������С */
	private static final int BUFFER_SIZE = 1024*1024;
	
	/** ÿ�ζ�ȡ��Ϣ������ */
	private static final int FETCH_SIZE = 100000;
	
	/** broker�˿� */
	private static final int PORT = 9092;
	
	/** ���̷�������ʱ���Ե������� */
	private static final int MAX_ERROR_NUM = 2;
	
	/**
	 * ��ȡ����Ԫ����
	 * @param brokerList:�����������б�
	 * @param port:�������˿�
	 * @param topic:Ҫ��ȡ��Ϣ������
	 * @param partitionId:Ҫ��ȡ��Ϣ����ķ���
	 * @return ����Ԫ����
	 */
	private PartitionMetadata fetchPartitionMetadata(List<String> brokerList,int port,String topic,int partitionId){
		SimpleConsumer consumer = null;
		TopicMetadataRequest metadataReq = null;
		TopicMetadataResponse metadataResp = null;
		List<TopicMetadata> topicMetadata = null;
		
		try{
			for(String host:brokerList){
				//1������һ�����������ڻ�ȡԪ������Ϣ��ִ����
				consumer = new SimpleConsumer(host, PORT, TIME_OUT, BUFFER_SIZE, "fetch-metadata");
				//2���������������Ԫ���ݵ�request
				metadataReq = new TopicMetadataRequest(Arrays.asList(topic));
				//3�����ͻ�ȡ����Ԫ���ݵ�����
				metadataResp = consumer.send(metadataReq);
				//4����ȡ����Ԫ�����б�
				topicMetadata = metadataResp.topicsMetadata();
				//5������Ԫ�����б�����ȡָ��������Ԫ������Ϣ
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
	 * ��ȡ��Ϣƫ����
	 * @param consumer:������
	 * @param topic:����
	 * @param partition:����
	 * @param beginTime:��ʼʱ��
	 * @param clientName:�ͻ�������
	 * @return ��Ϣƫ����
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
	 * ���ѵķ���
	 * @param brokerList:�����������б�
	 * @param port:�������˿�
	 * @param topic:����
	 * @param partition:����
	 */
	public void consume(List<String > brokerList,int port,String topic,int partitionId){
		SimpleConsumer consumer = null;
		try{
			//1�����Ȼ�ȡָ��������Ԫ������Ϣ
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
		    
		    //2������һ����Ϣ����Ϊ������Ϣ������ִ����
		    consumer = new SimpleConsumer(leaderBroker,port,TIME_OUT,BUFFER_SIZE,clientId);
		    long lastOffset = getLastOffset(consumer, topic, partitionId, kafka.api.OffsetRequest.EarliestTime(), clientId);
		    int errorNum = 0;
		    kafka.api.FetchRequest fetchRequest = null;
		    FetchResponse fetchResponse = null;
		    while(lastOffset>-1){
		    	if(consumer==null){
		    		consumer=new SimpleConsumer(leaderBroker, port, TIME_OUT, BUFFER_SIZE, clientId);
		    	}
		        //3�������ȡ��Ϣ��request
		        fetchRequest = new FetchRequestBuilder().clientId(clientId).addFetch(topic, partitionId, lastOffset, FETCH_SIZE).build();
		        //4����ȡ��Ӧ������
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
