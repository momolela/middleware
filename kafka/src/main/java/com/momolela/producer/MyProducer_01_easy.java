package com.momolela.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 简单的使用，创建参数，创建生产者对象，执行发送数据，关闭资源
 */
public class MyProducer_01_easy {
    public static void main(String[] args) {
        Properties props = new Properties();
        // kafka 集群，broker-list
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.10.2.81:9092,10.10.2.82:9092,10.10.2.109:9092");
        // 应答级别 默认all
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // 重试次数 默认3次
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 批次大小 默认16k
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 等待时间 默认1ms
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // RecordAccumulator 缓冲区大小 默认32m
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // key，value的序列化类
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 发送数据，执行时长<1ms
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("suntest", "suntest-msg-" + i));
        }

        // 关闭资源很重要，上面的配置，没到1ms或者没到16k，手动执行close()会把内存的东西都清理掉，说明批量发送一次结束
        producer.close();

    }
}
