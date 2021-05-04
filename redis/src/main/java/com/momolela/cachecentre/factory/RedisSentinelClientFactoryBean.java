package com.momolela.cachecentre.factory;

import com.momolela.cachecentre.clients.RedisClient;
import com.momolela.cachecentre.clients.sentinel.RedisSentinelClient;
import com.momolela.cachecentre.config.sentinel.RedisSentinelClientConfig;
import com.momolela.cachecentre.exception.RedisInitializerException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.FactoryBean;

public class RedisSentinelClientFactoryBean implements FactoryBean<RedisClient> {

    RedisSentinelClientConfig redisClientConfig = new RedisSentinelClientConfig();

    @Override
    public RedisClient getObject() throws Exception {
        if (StringUtils.isNotBlank(redisClientConfig.getServerConfString())) {
            return new RedisSentinelClient(redisClientConfig);
        } else {
            throw new RedisInitializerException("RedisSentinelClient init parameter redisClientConfig is empty, please check spring config file!");
        }
    }

    @Override
    public Class<?> getObjectType() {
        return RedisSentinelClient.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    // 通过 spring 和类的 set 方法进行配置赋值
    public void setMasterName(String masterName) {
        this.redisClientConfig.setMasterName(masterName);
    }

    public void setPassword(String password) {
        this.redisClientConfig.setPassword(password);
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

    public void setConnectionTimeout(int connectionTimeout) {
        this.redisClientConfig.setConnectionTimeout(connectionTimeout);
    }

    public void setSoTimeout(int soTimeout) {
        this.redisClientConfig.setSoTimeout(soTimeout);
    }

    public void setDbIndex(int dbIndex) {
        this.redisClientConfig.setDbIndex(dbIndex);
    }

    public void setServerConfString(String serverConfString) {
        this.redisClientConfig.setServerConfString(serverConfString);
    }

    public void setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
        this.redisClientConfig.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
    }

    public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
        this.redisClientConfig.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
    }

    public void setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
        this.redisClientConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
    }

    public void setTestOnBorrow(boolean testOnBorrow) {
        this.redisClientConfig.setTestOnBorrow(testOnBorrow);
    }

    public void setTestWhileIdle(boolean testWhileIdle) {
        this.redisClientConfig.setTestWhileIdle(testWhileIdle);
    }
}
