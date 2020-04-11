package com.momolela.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义的分区器，回顾一下生产者发送消息的流程：producer调用send()-->interceptors（拦截器）-->serializer（序列化器）-->partitioner（分区器）
 */
public class MyPartitioner implements Partitioner {

    /**
     * 自定义分区器的核心实现，就是当没有指定partition的时候，用于指定生产者生成的数据指定发送到哪个分区。具体实现可以参考DefaultPartitioner
     * @param topic
     * @param key
     * @param keyBytes 说明了是经过了序列化器之后的数据
     * @param value
     * @param valueBytes 说明了是经过了序列化器之后的数据
     * @param cluster
     * @return 分区号
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
