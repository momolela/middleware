package com.momolela.consumer;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.IOException;

public class JmsConsumerTopic {

    private static final String ACTIVEMQ_URL = "tcp://192.168.56.101:61616";
    private static final String TOPIC_NAME = "queue01";

    public static void main(String[] args) throws JMSException, IOException {
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
        // 5、创建消费者
        MessageConsumer messageConsumer = session.createConsumer(topic);

        // 6、这里使用receive主动消费
//        while (true) {
//            TextMessage textMessage = (TextMessage) messageConsumer.receive(4000L);
//            if (textMessage != null) {
//                System.out.println("消费者收到的消息是：：：" + textMessage.getText());
//            } else {
//                break;
//            }
//        }

        // 6、这里通过监听的方式进行消费
        messageConsumer.setMessageListener(message -> {
            TextMessage textMessage = (TextMessage) message;
            if (textMessage != null && textMessage instanceof TextMessage) {
                try {
                    System.out.println("消费者收到的消息是：：：" + textMessage.getText());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
        System.in.read(); // 保持程序不关闭，随意输入退出程序

        // 7、关闭资源
        messageConsumer.close();
        session.close();
        connection.close();
    }
}
