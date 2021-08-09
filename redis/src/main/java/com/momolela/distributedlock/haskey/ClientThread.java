package com.momolela.distributedlock.haskey;

import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;

public class ClientThread implements Runnable {

    private int clientNum;
    private String clientName;
    private String proNumKey = "product";
    private String clientListKey = "clientList";
    private Jedis jedis;
    private RedisBaseDistributedLock redisBaseDistributedLock;

    public ClientThread(int clientNum) {
        this.clientNum = clientNum;
        this.clientName = "客户编号=" + this.clientNum;
        initJedis();
    }

    private void initJedis() {
        Jedis jedis = new Jedis("127.0.0.1", 6379);
        jedis.auth("bship");
        this.jedis = jedis;
        this.redisBaseDistributedLock = new RedisBaseDistributedLock(this.jedis, "lock", 5000, 5000);
    }

    @Override
    public void run() {

        while (true) {
            try {
                TimeUnit.MILLISECONDS.sleep((int) (Math.random() * 5000)); // 随机睡眠一下
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // 先判断是否还有商品
            if (Integer.valueOf(this.jedis.get(this.proNumKey)) > 0) {
                if (redisBaseDistributedLock.lock(this.clientName)) {
                    Integer proNum = Integer.valueOf(this.jedis.get(this.proNumKey));
                    if (proNum > 0) {
                        this.jedis.decr(proNumKey); // 商品减1
                        this.jedis.sadd(clientListKey, this.clientName); // 增加抢购成功的客户端
                    }
                    redisBaseDistributedLock.unLock(this.clientName);
                    break;
                }
            } else {
                break;
            }
        }

        redisBaseDistributedLock = null;
        this.jedis.close();
    }
}
