package cn.bit.tao.topicmanager;

import java.util.Properties;

import org.junit.Test;

/**
 *@author  Tao wenjun
 *����TopicManager
 */

public class TestManager {
	/*
	 * ���Դ�������
	 */
	static String topic="hellokafka";
	@Test
	public void testCreateTopic(){
		TopicManager.createTopic(topic, 2, 2, new Properties());
		TopicManager.topicList();
	}
	
	/*
	 * �����޸�����������Ϣ
	 */
	@Test
	public void testChangeTopic(){
		Properties properties = new Properties();
		properties.setProperty("flush.messages", "2");
		TopicManager.modifyTopicConfig(topic, properties);
		TopicManager.topicInfo(topic);
	}
	
	/*
	 * ��������������
	 */
	@Test
	public void testAddPartitions(){
		TopicManager.addPartitions(topic, 3, "0:1,1:0,0:1", true);
		TopicManager.topicInfo(topic);
	}
	
	/*
	 * ���Է��������ط���
	 */
	@Test
	public void testPartitionAssignment(){
		TopicManager.partitionAssignment(topic, 2, 1);
		TopicManager.topicInfo(topic);
	}
	
	/*
	 * ����ɾ������
	 */
	@Test
	public void testDeleteTopic(){
		TopicManager.deleteTopic(topic);
		TopicManager.topicList();
	}
}
