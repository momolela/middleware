package com.momolela.pipeline.spring;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import io.reactivex.annotations.Nullable;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:config/spring/spring-main.xml")
public class usePiepelineInSpring {
	
	@Autowired
	private RedisTemplate redisTemplate;
	
	@Test
	public void testPiepeline(){
		
		// redistemplate的方式
//		long start = System.currentTimeMillis();
//		SessionCallback callback = operations -> {
//			for (int i = 1; i <= 50000; i++) {
//				operations.boundValueOps("pipeline_key_" + i).set("pipeline_value_" + i * 2);
//				operations.boundValueOps("pipeline_key_" + i).get();
//			}
//			return null;
//		};
//		List syncAndReturnAll = redisTemplate.executePipelined(callback);
//		long end = System.currentTimeMillis();
//		System.out.println("消耗时间（ms）："+(end-start));
//		System.out.println("读写操作次数："+syncAndReturnAll.size());
		
		// redistemplate中jedis的方式
		List<Long> List = redisTemplate.executePipelined(new RedisCallback<Long>() {
            @Nullable
            @Override
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                connection.openPipeline();
               for (int i = 0; i < 10000; i++) {
                    String key = "123" + i;
                    connection.zCount(key.getBytes(), 0,Integer.MAX_VALUE);
                }
                return null;
            }
        });
		System.out.println(List);
		
	}
	
	
}
