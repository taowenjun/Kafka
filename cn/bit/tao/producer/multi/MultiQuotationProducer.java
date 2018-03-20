package cn.bit.tao.producer.multi;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import cn.bit.tao.producer.entity.StockQuotationInfo;

/**
 *@author  tao wenjun
 *@date 2018年3月20日
 *多线程生产者
 */

public class MultiQuotationProducer {

	private static final Logger LOG=Logger.getLogger(MultiQuotationProducer.class);
	
	/** 设置实例生产消息的总数 */
	private static final int MSG_SIZE=100;
	
	/** 主题名称  */
	private static final String TOPIC="stock-quotation";
	
	/** Kafka集群 */
	private static final String BROKER_LIST="10.108.21.2:9092,10.108.23.236:9092";
	
	/**  生产者 */
	private static KafkaProducer<String, String> producer = null;
	
	static{
		//1、构造用于实例化KafkaProducer的Properties信息
		Properties configs = initConfig();
		//2、初始化一个KafkaProducer
		producer = new KafkaProducer<String, String>(configs);
	}
	
	/**
	 * 初始化Kafka配置	
	 * @return 配置信息
	 */
	private static Properties initConfig(){
		Properties properties = new Properties();
		//Kafka broker列表
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
		//设置序列化类
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		return properties;
	}
	
	/**
	 * 生产股票行情信息
	 * @return 一条股票行情信息
	 */
	private static StockQuotationInfo createQuotationInfo(){
		StockQuotationInfo quotationInfo = new StockQuotationInfo();
		//随机产生1到10之间的整数，然后与600100相加组成股票代码
		Random random = new Random();
		Integer stockCode = 600100+random.nextInt(10);
		//随机产生一个0到1之间的浮点数
		float ran = (float)Math.random();
		//设置涨停规则
		if(ran/2<0.4){
			ran=-ran;
		}
		DecimalFormat decimalFormat = new DecimalFormat(".00");
	    quotationInfo.setCurrentPrice(Float.valueOf(decimalFormat.format(11+ran))); //设置最新价在11元左右
	    quotationInfo.setPreClosePrice(11.80f);//设置昨日收盘价为固定值
	    quotationInfo.setOpenPrice(11.5f);//设置开盘价
	    quotationInfo.setLowPrice(10.5f);//设置最低价
	    quotationInfo.setHighPrice(12.5f);//设置最高价
	    quotationInfo.setStockCode(stockCode.toString());//设置股票代码
	    quotationInfo.setTradeTime(System.currentTimeMillis());//设置交易时间
	    quotationInfo.setStockName("股票-"+stockCode);//设置股票名称
	    return quotationInfo;
	}
	
	public static void main(String[] args) {
		ProducerRecord<String, String> record = null;
		StockQuotationInfo quotationInfo = null;
		ExecutorService executor = Executors.newFixedThreadPool(5);
		try{
			int num=0;
			for(int i=0;i<MSG_SIZE;i++){
				quotationInfo = createQuotationInfo();
				record=new ProducerRecord<String, String>(TOPIC, null,quotationInfo.getTradeTime(),
						quotationInfo.getStockCode(),quotationInfo.toString());
				
				executor.submit(new KafkaProducerThread(producer, record));
				if(num++%10==0){
					Thread.sleep(2000L);
				}
			}
		}catch(InterruptedException e){
			LOG.error("send message occurs exception",e);
		}finally{
			producer.close();
		}
	}

}
