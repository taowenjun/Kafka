#log4j与Kafka整合

#创建主题
[root@master kafka]# bin/kafka-topics.sh --create --zookeeper 10.108.21.2:2181,10.108.21.236:2181 --topic kafka-log4j --partitions 1 --replication-factor 1

#运行日志输出程序后，查看Kafka中采集的日志信息
[root@master kafka]# bin/kafka-run-class.sh kafka.tools.DumpLogSegments --files /tmp/kafka-logs/kafka-log4j-0/00000000000000000000.log --print-data-log
Dumping /tmp/kafka-logs/kafka-log4j-0/00000000000000000000.log
Starting offset: 0
offset: 0 position: 0 CreateTime: 1521890322076 isvalid: true payloadsize: 94 magic: 1 compresscodec: NONE crc: 3765369369 payload: 2018-03-24 19:18:41 [ERROR]-[cn.bit.tao.log4jproducer.Log4jProducer] this is a error message

offset: 1 position: 128 CreateTime: 1521890335887 isvalid: true payloadsize: 94 magic: 1 compresscodec: NONE crc: 1786807377 payload: 2018-03-24 19:18:55 [ERROR]-[cn.bit.tao.log4jproducer.Log4jProducer] this is a error message

offset: 2 position: 256 CreateTime: 1521890472462 isvalid: true payloadsize: 94 magic: 1 compresscodec: NONE crc: 2379673937 payload: 2018-03-24 19:21:12 [ERROR]-[cn.bit.tao.log4jproducer.Log4jProducer] this is a error message
