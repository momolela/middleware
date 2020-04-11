package com.momolela.transaction.payment;

import java.util.List;

import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class PayMent {
	static final Jedis jedis = new Jedis("127.0.0.1", 6379);
	
	@Test
	public void payTest() throws InterruptedException{
		jedis.auth("bship");
		
		boolean resultValue = payMent(10);
		System.out.println("交易结果（事务结果）："+resultValue);
		
		int balance = Integer.parseInt(jedis.get("balance"));
		int debt = Integer.parseInt(jedis.get("debt"));
		
		System.out.printf("balance：%d，debt：%d",balance,debt);
	}

	/**
	 * 支付操作，只考虑了外部改值，余额不足的情况，导致支付失败
	 * @param needPay
	 * @return
	 * @throws InterruptedException
	 */
	private boolean payMent(int needPay) throws InterruptedException {
		int balance;	// 余额
		int debt;	// 负债
		
		jedis.set("balance", "100");
		jedis.set("debt", "0");
		
		jedis.watch("balance","debt");	// 监控两个key，如果外部改了，直接放弃，说明支付失败
		
		balance = Integer.parseInt(jedis.get("balance"));
		
		if(balance<needPay){
			jedis.unwatch();
			System.out.println("余额不足");	// 余额不足支付失败
			return false;
		}
		
		Transaction ts = jedis.multi();
		ts.decrBy("balance", needPay);
		Thread.sleep(5000);
		ts.incrBy("debt", needPay);
		List<Object> list = ts.exec();
		
		return !list.isEmpty();
	}
}
