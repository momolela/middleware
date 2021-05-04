package com.momolela.cachecentre.config.cluster;

import com.momolela.cachecentre.config.BaseRedisClientConfig;
import org.apache.commons.lang3.StringUtils;

public class RedisClusterClientConfig extends BaseRedisClientConfig {

    private String clusterConfString; // cluster-ip 配置 String

    private int maxRedirects; // 最大失败调转次数

    private String password; // 连接 redis 密码

    public String getClusterConfString() {
        return clusterConfString;
    }

    public void setClusterConfString(String clusterConfString) {
        this.clusterConfString = clusterConfString;
    }

    public int getMaxRedirects() {
        return maxRedirects;
    }

    public void setMaxRedirects(int maxRedirects) {
        this.maxRedirects = maxRedirects;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        if (StringUtils.isNotBlank(password)) {
            this.password = password;
        }
    }

}
