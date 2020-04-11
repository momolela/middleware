package com.momolela.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyConsumer_01_easy {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.10.2.81:9092,10.10.2.82:9092,10.10.2.109:9092");
        // 自动提交开启，当消费后自动提交offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 1s提交一次
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        // 反序列化参数
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 消费者组id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sun");

        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("suntest", "haha"));

        while (true) {
            ConsumerRecords<Object, Object> records = consumer.poll(100);
            for (ConsumerRecord<Object, Object> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}
