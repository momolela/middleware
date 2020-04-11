package com.momolela.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 自定义分区器，在参数中指定分区器
 */
public class MyProducer_04_customPartitioner {
    public static void main(String[] args) {
        // 指定必须的参数，其他参数都是默认值
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.10.2.81:9092,10.10.2.82:9092,10.10.2.109:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // 指定用哪个分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.momolela.partitioner.MyPartitioner");

        // 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 发送数据，执行时长<1ms
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("suntest", "suntest-msg-" + i), (recordMetadata, e) -> {
                if (e == null) {
                    System.out.println(recordMetadata.partition()); // 打印查看是否为自定义分区器中指定的分区号
                } else {
                    e.printStackTrace();
                }
            });
        }

        // 关闭资源很重要，上面的配置，没到1ms或者没到16k，手动执行close()会把内存的东西都清理掉，说明批量发送一次结束
        producer.close();
    }
}
