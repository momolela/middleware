package com.momolela.messagequeue;

import java.util.List;

import redis.clients.jedis.Jedis;

public class RedisConsumer extends Thread{
	
	Jedis jedis = new Jedis("127.0.0.1", 6379);
	
	@Override
	public void run() {
		jedis.auth("bship");
		while(true) {
            // 阻塞式brpop，List中无数据时阻塞，参数0表示一直阻塞下去，直到List出现数据 
            List<String> list = jedis.brpop(0, "informList");
            for(String s : list) {
            	// 处理业务逻辑
                System.out.println(s);
            }
        }
	}
}
