package com.momolela.transaction.secondkill;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class MyRunable implements Runnable {

	Jedis jedis = new Jedis("127.0.0.1",6379);
	String userinfo;
	
	public MyRunable() {
		
	}

	public MyRunable(String userinfo) {
		this.userinfo = userinfo;
	}

	@Override
	public void run() {
		try {
			jedis.auth("bship");
			jedis.watch("watchkeys");
			
			int valint = Integer.parseInt(jedis.get("watchkeys"));	// 监控watchkeys这个key，如果外部有人改了这个值，事务撤销，抢购失败
			
			if(valint<=100&&valint>=1){	// 个数正常，继续抢购
				
				// 开启事务
				Transaction ts = jedis.multi();
				ts.incrBy("watchkeys", -1);
				List<Object> list = ts.exec();
				
				if(list==null||list.isEmpty()){	// 抢购失败，1，网络等导致请求抢购失败，2，被别人抢了...
					String failuserifo = "fail"+userinfo;
			        String failinfo = "用户：" + failuserifo + "商品争抢失败，抢购失败，请点击继续抢购";
			        System.out.println(failinfo);
			        jedis.setnx(failuserifo, failinfo);
				}else{
					for(Object succ : list){
			            String succuserifo ="succ"+succ.toString() + userinfo ;
			            String succinfo = "用户：" + succuserifo + "抢购成功，当前抢购成功人数:" + (1-(valint-100));
			            System.out.println(succinfo);
			            jedis.setnx(succuserifo, succinfo);
			       }
				}
			}else{
				String failuserifo ="kcfail" + userinfo;
			    String failinfo1 = "用户：" + failuserifo + "商品被抢购完毕，抢购失败";
			    System.out.println(failinfo1);
			    jedis.setnx(failuserifo, failinfo1);
			    return;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jedis.close();
		}
	}
}
