package com.momolela.pubsub.redistemplate;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

@Component
public class Subscribe1 implements MessageListener {
	
	@Autowired
    private RedisTemplate redisTemplate;

	@Override
	public void onMessage(Message message, byte[] pattern) {
		
		RedisSerializer<?> serializer = redisTemplate.getValueSerializer();
		Object msg = serializer.deserialize(message.getBody());
		Object topic = serializer.deserialize(message.getChannel());
		
		System.out.println("我是Subscribe1，监听的频道是："+topic+"，我收到消息："+msg);
	}

}
