package com.momolela.cachecentre.factory;

import com.momolela.cachecentre.clients.RedisClient;
import com.momolela.cachecentre.clients.cluster.RedisClusterClient;
import com.momolela.cachecentre.config.cluster.RedisClusterClientConfig;
import com.momolela.cachecentre.exception.RedisInitializerException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.FactoryBean;

public class RedisClusterClientFactoryBean implements FactoryBean<RedisClient> {

    RedisClusterClientConfig redisClientConfig = new RedisClusterClientConfig();

    @Override
    public RedisClient getObject() throws Exception {
        if (StringUtils.isNotBlank(redisClientConfig.getClusterConfString())) {
            return new RedisClusterClient(redisClientConfig);
        } else {
            throw new RedisInitializerException("RedisClusterClient init parameter clusterConfString is empty, please check spring config file!");
        }
    }

    @Override
    public Class<?> getObjectType() {
        return RedisClusterClient.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    // 通过 spring 和类的 set 方法进行配置赋值
    public void setConnectionTimeout(int connectionTimeout) {
        this.redisClientConfig.setConnectionTimeout(connectionTimeout);
    }

    public void setSoTimeout(int soTimeout) {
        this.redisClientConfig.setSoTimeout(soTimeout);
    }

    public void setMaxTotal(int maxTotal) {
        this.redisClientConfig.setMaxTotal(maxTotal);
    }

    public void setMaxIdle(int maxIdle) {
        this.redisClientConfig.setMaxIdle(maxIdle);
    }

    public void setMinIdle(int minIdle) {
        this.redisClientConfig.setMinIdle(minIdle);
    }

    public void setMaxWaitMillis(int maxWaitMillis) {
        this.redisClientConfig.setMaxWaitMillis(maxWaitMillis);
    }

    public void setMaxRedirects(int maxRedirects) {
        this.redisClientConfig.setMaxRedirects(maxRedirects);
    }

    public void setPassword(String password) {
        this.redisClientConfig.setPassword(password);
    }

    public void setClusterConfString(String clusterConfString) {
        this.redisClientConfig.setClusterConfString(clusterConfString);
    }
}
