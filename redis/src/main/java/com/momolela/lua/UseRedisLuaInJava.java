package com.momolela.lua;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import redis.clients.jedis.Jedis;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:config/spring/spring-main.xml")
public class UseRedisLuaInJava {
	@Autowired
	private RedisTemplate redisTemplate;
	
	@Test
	public void test(){
		// 通过redisTemplate得到jedis执行lua
//		Object returnObj = redisTemplate.execute(new RedisCallback<Object>() {
//			@Override
//	        public Object doInRedis(RedisConnection connection) {
//	            Jedis jedis = (Jedis) connection.getNativeConnection();
//	            return jedis.eval("return 'sunzhaojiang'");
//	        }
//	    }, true);
//		System.out.println(returnObj);
		
		// redisTemplate直接调用lua
		List<String> keyList = new ArrayList();
		keyList.add("firstName");
		keyList.add("LastName");
		String[] argvMap = {"zhaojiang","sun"};
		DefaultRedisScript<List> getRedisScript = new DefaultRedisScript<List>();
        getRedisScript.setResultType(List.class);
        getRedisScript.setScriptText("return {KEYS[1],ARGV[1],KEYS[2],ARGV[2]}");
//        getRedisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("luascript/LimitLoadTimes.lua")));
		Object returnObj = redisTemplate.execute(getRedisScript, keyList, argvMap);
		System.out.println(returnObj);
	}
}
