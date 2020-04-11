package com.momolela.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * 生产者发送数据可以通过回调函数返回一些数据
 */
public class MyProducer_02_callback {
    public static void main(String[] args) {
        // 指定必须的参数，其他参数都是默认值
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.10.2.81:9092,10.10.2.82:9092,10.10.2.109:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("suntest", "suntest-msg-" + i), (recordMetadata, e) -> { // 回调函数可以返回发送消息后的元数据
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
