package com.momolela.transaction.secondkill;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

import redis.clients.jedis.Jedis;

public class SecondKill {
	
	@Test
	public void startSecondKill(){
		final Jedis jedis = new Jedis("127.0.0.1",6379);
		jedis.auth("bship");
		
		jedis.set("watchkeys", "100");
		jedis.close();
		
		ExecutorService executor = Executors.newFixedThreadPool(20);	// 20个线程的线程池
		for(int i = 0;i<1000;i++){	// 总共1000个人去抢，20个人一批批的去抢。
			executor.execute(new MyRunable("user"+i));
		}
		executor.shutdown();
	}
}
