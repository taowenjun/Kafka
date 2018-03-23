#explanation

自定义分区器可以根据具体的业务需求来完成。
完成自定义分区器的编写后，需要在生产者的初始化Properties中添加ProducerConfig.PARTITIONER_CLASS_CONFIG这一项。
//properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,partitioner.class.getName());
