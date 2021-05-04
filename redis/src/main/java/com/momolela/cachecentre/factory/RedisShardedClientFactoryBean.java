package com.momolela.cachecentre.factory;

import com.momolela.cachecentre.clients.RedisClient;
import com.momolela.cachecentre.clients.sharded.RedisShardedClient;
import com.momolela.cachecentre.config.sharded.RedisShardedClientConfig;
import com.momolela.cachecentre.exception.RedisInitializerException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.FactoryBean;

public class RedisShardedClientFactoryBean implements FactoryBean<RedisClient> {

    RedisShardedClientConfig redisClientConfig = new RedisShardedClientConfig();

    @Override
    public RedisClient getObject() throws Exception {
        if (StringUtils.isNotBlank(redisClientConfig.getMasterConfString())) {
            return new RedisShardedClient(redisClientConfig);
        } else {
            throw new RedisInitializerException("RedisShardedClient init parameter masterConfString is empty, please check spring config file!");
        }
    }

    @Override
    public Class<?> getObjectType() {
        return RedisShardedClient.class;
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

    public void setMasterConfString(String masterConfString) {
        this.redisClientConfig.setMasterConfString(masterConfString);
    }

    public void setSlaveConfString(String slaveConfString) {
        this.redisClientConfig.setSlaveConfString(slaveConfString);
    }
}
