package com.momolela.producer;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class JmsProduceTopic {

    private static final String ACTIVEMQ_URL = "tcp://192.168.56.101:61616";
    private static final String TOPIC_NAME = "queue01";

    public static void main(String[] args) throws JMSException {
        // 1、创建连接工厂
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(ACTIVEMQ_URL);
        // 2、创建连接并启动
        Connection connection = activeMQConnectionFactory.createConnection();
        connection.start();
        // 3、创建会话，
        // 第一个参数：事务
        // 第二个参数：签收类型
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        // 4、创建目的地：topic
        Topic topic = session.createTopic(TOPIC_NAME);
        // 5、创建生产者对象
        MessageProducer producer = session.createProducer(topic);
        // 6、使用生产者对象发送消息到队列
        for (int i = 1; i <= 3; i++) {
            TextMessage textMessage = session.createTextMessage("发送第" + i + "条消息到MQ");
            producer.send(textMessage);
        }
        // 7、关闭资源
        producer.close();
        session.close();
        connection.close();
        System.out.println("消息发布到MQ完成");
    }
}
