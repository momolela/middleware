package com.momolela.messagequeue;

import org.junit.Test;

public class TestMq {
	
	@Test
	public void test(){
		RedisConsumer rc = new RedisConsumer();
		rc.start();
	}
}
