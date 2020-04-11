package com.momolela.messagequeue;

import redis.clients.jedis.Jedis;

public class RedisProducer implements Runnable {
	
	public void product(Jedis jedis,int i){
    	jedis.lpush("informList","orderIdadb_" + i);
	}

	@Override
	public void run() {
		Jedis jedis = new Jedis("127.0.0.1", 6379);
		jedis.auth("bship");
		for(int i = 0;i<5;i++){
			product(jedis,i);
		}
		jedis.close();
	}
	
	public static void main(String[] args) {
		RedisProducer redisProducer = new RedisProducer();
        Thread t1 = new Thread(redisProducer, "thread1");
        Thread t2 = new Thread(redisProducer, "thread2");
        Thread t3 = new Thread(redisProducer, "thread3");
        Thread t4 = new Thread(redisProducer, "thread4");
        Thread t5 = new Thread(redisProducer, "thread5");
        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();
	}
}
