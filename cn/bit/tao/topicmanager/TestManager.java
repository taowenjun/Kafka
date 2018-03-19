package cn.bit.tao.topicmanager;

import java.util.Properties;

import org.junit.Test;

/**
 *@author  Tao wenjun
 *测试TopicManager
 */

public class TestManager {
	/*
	 * 测试创建主题
	 */
	static String topic="hellokafka";
	@Test
	public void testCreateTopic(){
		TopicManager.createTopic(topic, 2, 2, new Properties());
		TopicManager.topicList();
	}
	
	/*
	 * 测试修改主题配置信息
	 */
	@Test
	public void testChangeTopic(){
		Properties properties = new Properties();
		properties.setProperty("flush.messages", "2");
		TopicManager.modifyTopicConfig(topic, properties);
		TopicManager.topicInfo(topic);
	}
	
	/*
	 * 测试添加主题分区
	 */
	@Test
	public void testAddPartitions(){
		TopicManager.addPartitions(topic, 3, "0:1,1:0,0:1", true);
		TopicManager.topicInfo(topic);
	}
	
	/*
	 * 测试分区副本重分配
	 */
	@Test
	public void testPartitionAssignment(){
		TopicManager.partitionAssignment(topic, 2, 1);
		TopicManager.topicInfo(topic);
	}
	
	/*
	 * 测试删除主题
	 */
	@Test
	public void testDeleteTopic(){
		TopicManager.deleteTopic(topic);
		TopicManager.topicList();
	}
}
