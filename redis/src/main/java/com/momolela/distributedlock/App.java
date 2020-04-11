package com.momolela.distributedlock;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import redis.clients.jedis.Jedis;

public class App {
	 
		public static void main(String[] args) {
			long starTime=System.currentTimeMillis();
			
			initPrduct();
			initClient();
			printResult();
			 
			long endTime=System.currentTimeMillis();
			long Time=endTime-starTime;
			System.out.println("程序运行时间： "+Time+"ms");   
		}
	 
		/**
		 * 输出结果
		 */
		public static void printResult() {
			Jedis jedis = new Jedis("127.0.0.1",6379);
			jedis.auth("bship");
			Set<String> set = jedis.smembers("clientList");
	 
			int i = 1;
			for (String value : set) {
				System.out.println("第" + i++ + "个抢到商品，" + value + " ");
			}
			jedis.close();
		}
	 
		/**
		 * 初始化顾客开始抢商品
		 */
		public static void initClient() {
			ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
			int clientNum = 10000;// 模拟客户数目
			for (int i = 0; i < clientNum; i++) {
				cachedThreadPool.execute(new PessClientThread(i));
			}
			cachedThreadPool.shutdown();
	 
			while (true) {
				if (cachedThreadPool.isTerminated()) {
					System.out.println("所有的线程都结束了！");
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	 
		/**
		 * 初始化商品个数
		 */
		public static void initPrduct() {
			int prdNum = 100;// 商品个数
			String key = "prdNum";
			String clientList = "clientList";// 抢购到商品的顾客列表
			Jedis jedis = new Jedis("127.0.0.1",6379);
	 
			if (jedis.exists(key)) {
				jedis.del(key);
			}
	 
			if (jedis.exists(clientList)) {
				jedis.del(clientList);
			}
	 
			jedis.set(key, String.valueOf(prdNum));// 初始化
			jedis.close();
		}
	 
	}
