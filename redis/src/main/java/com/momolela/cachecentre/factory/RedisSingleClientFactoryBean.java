package com.momolela.cachecentre.factory;

import com.momolela.cachecentre.clients.RedisClient;
import com.momolela.cachecentre.clients.single.RedisSingleClient;
import com.momolela.cachecentre.config.single.RedisSingleClientConfig;
import org.springframework.beans.factory.FactoryBean;

public class RedisSingleClientFactoryBean implements FactoryBean<RedisClient> {

    RedisSingleClientConfig redisSingleClientConfig = new RedisSingleClientConfig();

    @Override
    public RedisClient getObject() throws Exception {
        return null;
    }

    @Override
    public Class<?> getObjectType() {
        return RedisSingleClient.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
