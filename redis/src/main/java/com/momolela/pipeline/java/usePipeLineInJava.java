package com.momolela.pipeline.java;

import java.util.HashMap;

import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class usePipeLineInJava {
	
	@Test
	public void testPipeline(){
		// 连接redis客户端，创建管道对象
		Jedis jedis = new Jedis("127.0.0.1", 6379);
		jedis.auth("bship");
		Pipeline pl = jedis.pipelined();
		
		// 把redis的命令执行结果都存在map中
		HashMap<String, Response<String>> map = new HashMap<String,Response<String>>(20);
		HashMap<String, Response<Long>> map1 = new HashMap<String,Response<Long>>(20);

		// 把所有要执行的命令都放进管道对象中
		map.put("shiqing1", pl.set("shiqing1", "tiancai"));
		map1.put("shi", pl.sadd("shi", "frefbrier"));
		map.put("shiqing1value", pl.get("shiqing1"));
		map1.put("shiqing", pl.lpush("shiqing", "name"));
		map1.put("qing", pl.hsetnx("qing", "name", "sq"));
		
		// 一次性执行所有命令
		pl.sync();
		
		System.out.println(map.get("shiqing1").get());
		System.out.println(map1.get("shi").get());
		System.out.println(map.get("shiqing1value").get());
		System.out.println(map1.get("shiqing").get());
		System.out.println(map1.get("qing").get());
		
		// 关掉连接和管道
		try {
			pl.close();
			jedis.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
