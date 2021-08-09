package com.momolela.distributedlock.haskey;

import org.springframework.util.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class RedisBaseDistributedLock {

    private Jedis jedis;
    private String lockKey;
    private long getLockTimeout;
    private long lockExpireTime;

    private SetParams setParams;

    public RedisBaseDistributedLock(Jedis jedis, String lockKey, long getLockTimeout, long lockExpireTime) {
        this.jedis = jedis;
        this.lockKey = lockKey;
        this.getLockTimeout = getLockTimeout;
        this.lockExpireTime = lockExpireTime;
        this.setParams = SetParams.setParams().nx().px(this.lockExpireTime);
    }

    /**
     * 阻塞式获取锁
     *
     * @param key
     * @return
     */
    public boolean lock(String key) {
        if (!StringUtils.isEmpty(key)) {
            try {
                long start = System.currentTimeMillis();
                for (; ; ) {
                    // 在循环中做阻塞直接设置锁 key
                    String result = jedis.set(this.lockKey, key, this.setParams);
                    if ("OK".equals(result)) {
                        return true;
                    }
                    // 如果设置锁 key 失败，判断锁获取是否超时
                    long cost = System.currentTimeMillis() - start;
                    if (cost > getLockTimeout) {
                        return false;
                    }
                    // 设置短暂睡眠时间
                    try {
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
        return false;
    }

    /**
     * 解锁，必须解自己的锁，不能错乱，所以要保持原子性，用 lua 配合
     *
     * @param key
     * @return
     */
    public boolean unLock(String key) {
        String script = "if redis.call('get',KEYS[1]) == ARGV[1] then" +
                "   return redis.call('del',KEYS[1]) " +
                "else" +
                "   return 0 " +
                "end";
        try {
            Object result = jedis.eval(script, Collections.singletonList(this.lockKey), Collections.singletonList(key));
            if ("1".equals(result)) {
                return true;
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return false;
    }

    /**
     * 判断是否可以获得锁
     *
     * @param key
     * @return
     */
    public boolean canReEnter(String key) {
        try {
            String value = jedis.get(this.lockKey);
            if (StringUtils.isEmpty(value) || value.equals(key)) {
                return true;
            } else {
                return false;
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return false;
    }

}
