package com.momolela.pubsub.redistemplate;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class Publisher {
	
	@Autowired
	private RedisTemplate redisTemplate;

	public void sendMsg(String channel,String msg){
		redisTemplate.convertAndSend(channel, msg);
	}
}
