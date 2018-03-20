package cn.bit.tao.topicmanager;

import java.util.Properties;

import org.apache.kafka.common.security.JaasUtils;

import kafka.admin.AdminUtils;
import kafka.admin.BrokerMetadata;
import kafka.admin.TopicCommand;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import scala.collection.Map;
import scala.collection.Seq;



/**
 *@author  Tao wenjun
 *Kafka主题管理
 */

public class TopicManager {
	
	/** 连接ZK */
	private static final String ZK_CONNECT="10.108.21.2:2181,10.108.23.236:2181";
	/** session过期时间 */
	private static final int SESSION_TIMEOUT=30000;
	/** 连接超时时间 */
	private static final int CONNECT_TIMEOUT=30000;
	
	/*
	 * 创建主题
	 * @param topic:需要创建的主题
	 * @param partition:主题的分区数
	 * @param replica:分区副本数
	 * @param properties:配置信息
	 */
	public static void createTopic(String topic,int partition,int replica,Properties properties){
		ZkUtils zkUtils = null;
		try{
			zkUtils=ZkUtils.apply(ZK_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT, JaasUtils.isZkSecurityEnabled());
			if(!AdminUtils.topicExists(zkUtils, topic)){
				AdminUtils.createTopic(zkUtils, topic, partition, replica, properties, AdminUtils.createTopic$default$6());
			}else{
				System.out.println("Topic already exists");
			}	
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			zkUtils.close();
		}
	}
	
	/*
	 * 修改主题级别配置
	 * @param topic:主题名称
	 * @param properties:配置信息
	 */
	public static void modifyTopicConfig(String topic,Properties properties){
		ZkUtils zkUtils=null;
		try{
			//实例化ZkUtils
			zkUtils=ZkUtils.apply(ZK_CONNECT, SESSION_TIMEOUT,CONNECT_TIMEOUT,JaasUtils.isZkSecurityEnabled());
			//获取当前已有的配置，主题级别，指定配置类型为Topic
			Properties curProp = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
//		    Set<Entry<Object, Object>> entrySet = curProp.entrySet();
//		    if(entrySet.isEmpty()){
//		    	System.out.println("Config info is null");
//		    }
//		    for(Entry<Object,Object> entry:entrySet){
//		    	System.out.println(entry.getKey()+"-->"+entry.getValue());
//		    }
		    curProp.putAll(properties);
		    AdminUtils.changeTopicConfig(zkUtils, topic, curProp);
//		    Properties changedProp = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
//		    Set<Entry<Object, Object>> entrySet1 = changedProp.entrySet();
//		    for(Entry<Object,Object> entry:entrySet1){
//		    	System.out.println(entry.getKey()+"-->"+entry.getValue());
//		    }
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			zkUtils.close();
		}
	}
	
	/*
	 * 增加分区
	 * @param topic:需要添加分区的主题
	 * @param partitions:添加后的分区总数
	 * @param replicas:分区副本放置信息
	 * @param auto:是否分区副本自动分配
	 */
	public static void addPartitions(String topic,int partitions,String replicas,boolean auto){
		ZkUtils zkUtils=null;
		try{
			zkUtils=ZkUtils.apply(ZK_CONNECT, CONNECT_TIMEOUT, SESSION_TIMEOUT, JaasUtils.isZkSecurityEnabled());
			AdminUtils.addPartitions(zkUtils, topic, partitions, replicas, auto, AdminUtils.addPartitions$default$6());
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			zkUtils.close();
		}
	}
	
	/*
	 * 分区副本重新配置
	 * @param topic:主题
	 * @param partitions:分区数
	 * @param replicas:副本数
	 */
	public static void partitionAssignment(String topic,int partitions,int replicas){
		ZkUtils zkUtils=null;
		try{
			//实例化ZkUtils
			zkUtils=ZkUtils.apply(ZK_CONNECT, CONNECT_TIMEOUT, SESSION_TIMEOUT, JaasUtils.isZkSecurityEnabled());
			//获取代理元数据信息
			Seq<BrokerMetadata> brokerMeta=(Seq<BrokerMetadata>) AdminUtils.getBrokerMetadatas(zkUtils, AdminUtils.getBrokerMetadatas$default$2(), AdminUtils.getBrokerMetadatas$default$3());
		    //生成分区副本分配方案
			Map<Object, Seq<Object>> replicaAssign = AdminUtils.assignReplicasToBrokers(brokerMeta, partitions, replicas, AdminUtils.assignReplicasToBrokers$default$4(), AdminUtils.assignReplicasToBrokers$default$5());
		    //修改分区副本分配方案
			AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, (scala.collection.Map<Object, scala.collection.Seq<Object>>) replicaAssign, null, true);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			zkUtils.close();
		}
	}
	
	/*
	 * 删除主题
	 * @param topic:要删除的主题
	 */
	public static void deleteTopic(String topic){
		ZkUtils zkUtils=null;
		try{
			zkUtils=ZkUtils.apply(ZK_CONNECT, CONNECT_TIMEOUT, SESSION_TIMEOUT, JaasUtils.isZkSecurityEnabled());
            AdminUtils.deleteTopic(zkUtils, topic);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			zkUtils.close();
		}
	}
	
	/*  TopicCommand的一些操作 */
	
	/*
	 * 查看某主题信息
	 * @param topic:需要查看的主题
	 */
	public static void topicInfo(String topic){
		String[] options = new String[]{  
			    "--describe",  
			    "--zookeeper",  
			    ZK_CONNECT,  
			    "--topic",  
			    topic,  
			};  
	    TopicCommand.main(options);  
	}
	
	/*
	 * 查看所有主题列表
	 */
	public static void topicList(){
		String[] options = new String[]{ 
				"--list", 
				"--zookeeper", 
				ZK_CONNECT};

	    TopicCommand.main(options);
	}
	
	
}
