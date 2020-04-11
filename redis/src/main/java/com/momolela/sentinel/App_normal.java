package com.momolela.sentinel;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

public class App_normal {
	
	@Test
	public void testSentinel(){
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(10);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMinIdle(5);
        
        // 哨兵信息
        Set<String> sentinels = new HashSet<>(Arrays.asList("10.10.2.78:26379","10.10.2.79:26379","10.10.2.83:26379"));
        // 创建连接池
        JedisSentinelPool pool = new JedisSentinelPool("mymaster", sentinels, jedisPoolConfig, "bship");
        // 获取客户端
        Jedis jedis = pool.getResource();
        jedis.select(4);
        // 执行两个命令
        jedis.set("sun", "haha");
        String value = jedis.get("sun");
        System.out.println(value);
        pool.close();
	}
	
}
