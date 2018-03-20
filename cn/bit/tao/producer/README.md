#kafka producer

为使程序运行起来首先要创建名为“stock-quotation”的topic

最终结果可进入${KAFKA_HOME}/bin执行如下命令
./kafka-run-class.sh kafka.tools.DumpLogSegments --file /tmp/kafka-logs/stock-quotation-1/00000000000000000000.log --print-data-log

Starting offset: 0
offset: 0 position: 0 CreateTime: 1521530865528 isvalid: true payloadsize: 60 magic: 1 compresscodec: NONE crc: 2909967091 keysize: 6 key: 600107 payload: 600107|鑲＄エ-600107|1521530865528|11.8|11.5|10.41|12.5|10.5
offset: 1 position: 100 CreateTime: 1521530865528 isvalid: true payloadsize: 59 magic: 1 compresscodec: NONE crc: 205437017 keysize: 6 key: 600103 payload: 600103|鑲＄エ-600103|1521530865528|11.8|11.5|10.2|12.5|10.5

可以看到生产的信息（乱码是SecureCRT的问题）
