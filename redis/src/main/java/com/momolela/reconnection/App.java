package com.momolela.reconnection;

import org.junit.Test;
import redis.clients.jedis.Jedis;

public class App {

    /**
     * 重新获取连接是支持断线重连的
     */
    @Test
    public void test() {
        for (int i = 0; i < 1000; i++) {
            Jedis jedis = null;
            try {
                jedis = new Jedis("127.0.0.1", 6379);
                jedis.auth("bship");
                jedis.select(1);
                jedis.set(i + "", i + "");
                Thread.sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (jedis != null)
                    jedis.close();
            }
        }
    }
}
