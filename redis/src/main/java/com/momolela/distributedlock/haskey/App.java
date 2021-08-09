package com.momolela.distributedlock.haskey;

import redis.clients.jedis.Jedis;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class App {

    public static void main(String[] args) {
        initProduct();
        initClient();
        printResult();
    }

    private static void initProduct() {
        // 初始化商品个数
        int proNum = 100;
        String proKey = "product";
        String clientListKey = "clientList";
        Jedis jedis = new Jedis("127.0.0.1", 6379);
        jedis.auth("bship");

        // 如果存在 key 先清空
        if (jedis.exists(proKey)) {
            jedis.del(proKey);
        }
        if (jedis.exists(clientListKey)) {
            jedis.del(clientListKey);
        }

        // 初始化数据
        jedis.set(proKey, String.valueOf(proNum));

        // 关闭资源
        jedis.close();
    }

    private static void initClient() {
        // 初始化客户端个数
        int clientNum = 1000;

        // 开线程抢购
        ExecutorService threadPool = Executors.newCachedThreadPool();
        for (int i = 0; i < clientNum; i++) {
            threadPool.execute(new ClientThread(i));
        }
        threadPool.shutdown();

        long start = System.currentTimeMillis();
        while (true) {
            if (threadPool.isTerminated()) {
                long cost = System.currentTimeMillis() - start;
                System.out.println("抢购线程耗费了 " + (cost / 1000) + " s");
                break;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }

    private static void printResult() {
        Jedis jedis = new Jedis("127.0.0.1", 6379);
        jedis.auth("bship");

        // 阻塞获取抢购到的客户端数据
        Set<String> clientList = jedis.smembers("clientList");

        int i = 1;
        for (String client : clientList) {
            System.out.println(client + ", 第" + i++ + "个抢到商品");
        }

        jedis.close();
    }
}
