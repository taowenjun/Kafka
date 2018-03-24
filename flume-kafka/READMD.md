
#创建主题
[root@master kafka]# bin/kafka-topics.sh --create --zookeeper master:2181,slave1:2181 --topic flume-kafka -partitions 1 -replication-factor 1
Created topic "flume-kafka".

#启动flume
[root@master flume]# bin/flume-ng agent --conf /root/software/flume/conf --conf-file conf/flume-kafka.properties --name agent -Dflume.root.logger=INFO,console
Info: Sourcing environment configuration script /root/software/flume/conf/flume-env.sh

#向日志文件中写数据
[root@master ~]# echo "hello world">>test.log

#Kafka进行消费
[root@master kafka]# bin/kafka-console-consumer.sh --bootstrap-server 10.108.21.2:9092 --topic flume-kafka
hello world