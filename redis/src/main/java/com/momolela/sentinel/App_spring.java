package com.momolela.sentinel;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.momolela.util.RedisCacheUtil;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:config/spring/spring-main.xml")
public class App_spring {
	
	@Autowired
    private RedisCacheUtil redisCacheUtil;
	
	@Test
	public void testSentinel(){
		redisCacheUtil.setCacheObject("zhao", "hehe");
		System.out.println(redisCacheUtil.getCacheObject("zhao"));
	}
	
}
