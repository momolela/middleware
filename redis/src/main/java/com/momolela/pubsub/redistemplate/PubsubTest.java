package com.momolela.pubsub.redistemplate;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:config/spring/spring-main.xml")
public class PubsubTest {

	@Autowired
	private Publisher publisher;

	@Test
	public void test() {
		publisher.sendMsg("topic.sun", "[这是频道发出的消息]");
	}
}
