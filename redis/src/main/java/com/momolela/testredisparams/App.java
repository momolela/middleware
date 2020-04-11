package com.momolela.testredisparams;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.momolela.util.RedisCacheUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:config/spring/spring-main.xml")
public class App {
	
	@Autowired
    private RedisCacheUtil redisCacheUtil;
	
	/**
	 * timeout貌似无效，因为不会自己断开
	 */
	@Test
	public void testTimeout(){
		try {
			redisCacheUtil.setCacheObject("sun", "1");
			Thread.sleep(20000);
			System.out.println(redisCacheUtil.getCacheObject("sun"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testIsDie(){
		JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "127.0.0.1", 6379, 2000, "bship");
		Jedis jedis = jedisPool.getResource();
		jedis.set("haha", "sunzj");
		jedis.close();
		
		int i = 0;
		
		while(i<100000){
			Jedis jedis1 = jedisPool.getResource();
			try {
				Thread.sleep(1000);
				System.out.println(jedis1.get("haha"));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}finally{
				jedis1.close();
			}
			
			i++;
			
			if(i%3600==0){
				try {
					Thread.sleep(3600000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
