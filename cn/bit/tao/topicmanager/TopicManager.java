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
 *Kafka�������
 */

public class TopicManager {
	
	/** ����ZK */
	private static final String ZK_CONNECT="10.108.21.2:2181,10.108.23.236:2181";
	/** session����ʱ�� */
	private static final int SESSION_TIMEOUT=30000;
	/** ���ӳ�ʱʱ�� */
	private static final int CONNECT_TIMEOUT=30000;
	
	/*
	 * ��������
	 * @param topic:��Ҫ����������
	 * @param partition:����ķ�����
	 * @param replica:����������
	 * @param properties:������Ϣ
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
	 * �޸����⼶������
	 * @param topic:��������
	 * @param properties:������Ϣ
	 */
	public static void modifyTopicConfig(String topic,Properties properties){
		ZkUtils zkUtils=null;
		try{
			//ʵ����ZkUtils
			zkUtils=ZkUtils.apply(ZK_CONNECT, SESSION_TIMEOUT,CONNECT_TIMEOUT,JaasUtils.isZkSecurityEnabled());
			//��ȡ��ǰ���е����ã����⼶��ָ����������ΪTopic
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
	 * ���ӷ���
	 * @param topic:��Ҫ��ӷ���������
	 * @param partitions:��Ӻ�ķ�������
	 * @param replicas:��������������Ϣ
	 * @param auto:�Ƿ���������Զ�����
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
	 * ����������������
	 * @param topic:����
	 * @param partitions:������
	 * @param replicas:������
	 */
	public static void partitionAssignment(String topic,int partitions,int replicas){
		ZkUtils zkUtils=null;
		try{
			//ʵ����ZkUtils
			zkUtils=ZkUtils.apply(ZK_CONNECT, CONNECT_TIMEOUT, SESSION_TIMEOUT, JaasUtils.isZkSecurityEnabled());
			//��ȡ����Ԫ������Ϣ
			Seq<BrokerMetadata> brokerMeta=(Seq<BrokerMetadata>) AdminUtils.getBrokerMetadatas(zkUtils, AdminUtils.getBrokerMetadatas$default$2(), AdminUtils.getBrokerMetadatas$default$3());
		    //���ɷ����������䷽��
			Map<Object, Seq<Object>> replicaAssign = AdminUtils.assignReplicasToBrokers(brokerMeta, partitions, replicas, AdminUtils.assignReplicasToBrokers$default$4(), AdminUtils.assignReplicasToBrokers$default$5());
		    //�޸ķ����������䷽��
			AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, (scala.collection.Map<Object, scala.collection.Seq<Object>>) replicaAssign, null, true);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			zkUtils.close();
		}
	}
	
	/*
	 * ɾ������
	 * @param topic:Ҫɾ��������
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
	
	/*  TopicCommand��һЩ���� */
	
	/*
	 * �鿴ĳ������Ϣ
	 * @param topic:��Ҫ�鿴������
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
	 * �鿴���������б�
	 */
	public static void topicList(){
		String[] options = new String[]{ 
				"--list", 
				"--zookeeper", 
				ZK_CONNECT};

	    TopicCommand.main(options);
	}
	
	
}
