package com.momolela.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 通过ProducerRecord的构造重载方法，可以实现不同的分区策略
 */
public class MyProducer_03_partitionStrategy {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.10.2.81:9092,10.10.2.82:9092,10.10.2.109:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            // 通过ProducerRecord的构造重载实现，消费者的不同分区策略
            // 1、ProducerRecord<> (String topic, String partition, ...); 指定了partition的时候，直接将数据发送到指定的partition中
            // 2、ProducerRecord<> (String topic, String key, String value); 没指定 partition 但有 key 的时候，将 key 的 hash 值与 topic 的 partition 数进行取余得到 partition 值
            // 3、ProducerRecord<> (String topic, String value); 既没有 partition 值又没有 key 值的情况下，第一次调用时随机生成一个整数（后面每次调用在这个整数上自增），将这个值与 topic 可用的 partition 总数取余得到 partition 值，也就是常说的 round-robin 算法。轮询插入；
            producer.send(new ProducerRecord<>("suntest", "suntest-msg-" + i), (recordMetadata, e) -> { // 这里采用的第3种，也就是最终是轮询插入分区
                if (e == null) {
                    System.out.println(recordMetadata.partition() + "----" + recordMetadata.offset());
                } else {
                    e.printStackTrace();
                }
            });
        }

        producer.close();
    }
}
