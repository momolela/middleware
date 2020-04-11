package com.momolela.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 同步发送数据到分区中，在调用send()方法之后调用get()方法进行阻塞，一般很少用，是在要求有序的时候，估计就不会选用kafka来做消息队列了
 */
public class MyProducer_05_synchroSend {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 指定必须的参数，其他参数都是默认值
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.10.2.81:9092,10.10.2.82:9092,10.10.2.109:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 发送数据，执行时长<1ms
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("suntest", "suntest-msg-" + i)).get(); // 因为.send方法执行后返回的是一个Future对象，可以通过.get()方法实现阻塞同步发送
        }

        // 关闭资源很重要，上面的配置，没到1ms或者没到16k，手动执行close()会把内存的东西都清理掉，说明批量发送一次结束
        producer.close();
    }
}
