package com.momolela.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
//import java.util.Collection;
import java.util.Map;
import java.util.Properties;

public class MyConsumer_04_commitOffset {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.10.2.81:9092,10.10.2.82:9092,10.10.2.109:9092");
        // 关闭自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sun");

        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("suntest", "haha"));

        while (true) {
            ConsumerRecords<Object, Object> records = consumer.poll(100);
            for (ConsumerRecord<Object, Object> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
            // 同步提交，当前线程会阻塞直到 offset 提交成功，会自动失败重试。由不可控因素导致，也会出现提交失败。而且吞吐量会受到很大的影响
            // consumer.commitSync();

            //异步提交，没有失败重试机制，故有可能提交失败
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                    if (e != null) {
                        System.err.println("Commit failed for" + offsets);
                    }
                }
            });

            // 无论是同步提交还是异步提交 offset，都有可能会造成数据的漏消费或者重复消费。
            // 先提交 offset 后消费，有可能造成数据的漏消费；
            // 而先消费后提交 offset，有可能会造成数据的重复消费。
            // 所以还支持自定义存储 offset：就是把offset存储到数据库或者本地文件里面，然后消费和提交offset可以做成事务。数据库里面存储：consumer group，topic，partition，offset
            // 修改订阅就可以
//            consumer.subscribe(Arrays.asList("suntest", "haha"), new ConsumerRebalanceListener() {
//                //该方法会在 Rebalance 之前调用
//                @Override
//                public void onPartitionsRevoked(Collection<TopicPartition> collection) {
//                    commitOffset(currentOffset); // 自己实现
//                }
//                //该方法会在 Rebalance 之后调用
//                @Override
//                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//                    currentOffset.clear(); // 自己实现
//                    for (TopicPartition partition : partitions) {
//                        consumer.seek(partition, getOffset(partition)); // 定位到最近提交的 offset 位置继续消费，自己实现
//                    }
//                }
//            });
        }
    }
}
