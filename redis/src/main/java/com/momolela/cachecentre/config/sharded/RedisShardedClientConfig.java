package com.momolela.cachecentre.config.sharded;

import com.momolela.cachecentre.config.BaseRedisClientConfig;

public class RedisShardedClientConfig extends BaseRedisClientConfig {

    private String masterConfString; // 主 ip 配置 String
    private String slaveConfString; // 从 ip 配置 String

    public String getMasterConfString() {
        return masterConfString;
    }

    public void setMasterConfString(String masterConfString) {
        this.masterConfString = masterConfString;
    }

    public String getSlaveConfString() {
        return slaveConfString;
    }

    public void setSlaveConfString(String slaveConfString) {
        this.slaveConfString = slaveConfString;
    }
}
