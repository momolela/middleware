package com.momolela.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

public class MyProducer_06_customInterceptor {
    public static void main(String[] args) throws InterruptedException {
        // 指定必须的参数，其他参数都是默认值
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.10.2.81:9092,10.10.2.82:9092,10.10.2.109:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 添加多个拦截器
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Arrays.asList("com.momolela.interceptor.TimeInterceptor", "com.momolela.interceptor.CounterInterceptor"));

        // 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            // 发送数据，执行时长<1ms
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<String, String>("suntest", "suntest-msg-" + i));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭资源很重要，上面的配置，没到1ms或者没到16k，手动执行close()会把内存的东西都清理掉，说明批量发送一次结束，这里会调用拦截器的close
            producer.close();
        }
    }
}
