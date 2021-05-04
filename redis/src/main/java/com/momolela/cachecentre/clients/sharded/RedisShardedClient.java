package com.momolela.cachecentre.clients.sharded;

import com.momolela.cachecentre.clients.RedisClient;
import com.momolela.cachecentre.config.sharded.RedisShardedClientConfig;
import com.momolela.cachecentre.exception.RedisAccessException;
import com.momolela.cachecentre.exception.RedisInitializerException;
import com.momolela.cachecentre.utils.CloseUtil;
import com.momolela.cachecentre.utils.TranscoderUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.util.SafeEncoder;

import java.util.*;

public class RedisShardedClient implements RedisClient {

    private static final Log LOG = LogFactory.getLog(RedisShardedClient.class);
    private ShardedJedisPool writePool; // 写池
    private ShardedJedisPool readPool; // 读池
    private RedisShardedClientConfig redisClientConfig; // redis 连接配置
    private JedisPoolConfig jedisPoolConfig; // redis 连接池配置

    /**
     * 加载配置构造器
     *
     * @param redisClientConfig
     */
    public RedisShardedClient(RedisShardedClientConfig redisClientConfig) {
        this.redisClientConfig = redisClientConfig;
        jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxWaitMillis(redisClientConfig.getMaxWaitMillis());
        jedisPoolConfig.setMaxTotal(redisClientConfig.getMaxTotal());
        jedisPoolConfig.setMaxIdle(redisClientConfig.getMaxIdle());
        jedisPoolConfig.setMinIdle(redisClientConfig.getMinIdle());
        init();
    }

    /**
     * 初始化redis连接
     */
    private void init() {
        LOG.info("redis sharded client init");
        List<JedisShardInfo> writeShards = new ArrayList<>();
        List<JedisShardInfo> readShards = new ArrayList<>();
        List<String> masterConfList = Arrays.asList(this.redisClientConfig.getMasterConfString().split("(?:\\s|,)+")); // 主 ip list
        for (String writeAddress : masterConfList) {
            if (StringUtils.isNotBlank(writeAddress)) {
                String[] ipPortArray = writeAddress.split(":");
                if (ipPortArray.length == 1) {
                    throw new RedisInitializerException(Arrays.toString(ipPortArray) + " is not include host:port or host:port:pass after split \":\"");
                } else {
                    String ip = ipPortArray[0];
                    int port = Integer.valueOf(ipPortArray[1]);
                    JedisShardInfo jedisShardInfo = new JedisShardInfo(ip, port);
                    jedisShardInfo.setConnectionTimeout(this.redisClientConfig.getConnectionTimeout());
                    jedisShardInfo.setSoTimeout(this.redisClientConfig.getSoTimeout());
                    LOG.info("write redis client connect ip:" + ip + ",port:" + port);
                    if (ipPortArray.length == 3 && StringUtils.isNotBlank(ipPortArray[2])) {
                        jedisShardInfo.setPassword(ipPortArray[2]);
                    }
                    writeShards.add(jedisShardInfo);
                }
            }
        }
        this.writePool = new ShardedJedisPool(this.jedisPoolConfig, writeShards);
        if (StringUtils.isNotBlank(this.redisClientConfig.getSlaveConfString()) && !this.redisClientConfig.getSlaveConfString().equals(this.redisClientConfig.getMasterConfString())) {
            List<String> slaveConfList = Arrays.asList(this.redisClientConfig.getSlaveConfString().split("(?:\\s|,)+")); // 从 ip list
            for (String readAddress : slaveConfList) {
                if (StringUtils.isNotBlank(readAddress)) {
                    String[] ipPortArray = readAddress.split(":");
                    if (ipPortArray.length == 1) {
                        throw new RedisInitializerException(Arrays.toString(ipPortArray) + " is not include host:port or host:port:pass after split \":\"");
                    } else {
                        String ip = ipPortArray[0];
                        int port = Integer.valueOf(ipPortArray[1]);
                        JedisShardInfo jedisShardInfo = new JedisShardInfo(ip, port);
                        jedisShardInfo.setConnectionTimeout(this.redisClientConfig.getConnectionTimeout());
                        jedisShardInfo.setSoTimeout(this.redisClientConfig.getSoTimeout());
                        LOG.info("read redis client connect ip:" + ip + ",port:" + port);
                        if (ipPortArray.length == 3 && StringUtils.isNotBlank(ipPortArray[2])) {
                            jedisShardInfo.setPassword(ipPortArray[2]);
                        }
                        readShards.add(jedisShardInfo);
                    }
                }
            }
            this.readPool = new ShardedJedisPool(this.jedisPoolConfig, readShards);
        }
    }

    @Override
    public String setObject(String key, Object value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.set(SafeEncoder.encode(key), TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long setObjectNx(String key, Object value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.setnx(SafeEncoder.encode(key), TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String setObjectEx(String key, int seconds, Object value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.setex(SafeEncoder.encode(key), seconds, TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public <T> T getObject(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return (T) TranscoderUtils.decodeObject(shardedJedis.get(SafeEncoder.encode(key)));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long delObject(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.del(SafeEncoder.encode(key));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Boolean existsObject(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.exists(SafeEncoder.encode(key));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long del(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.del(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Boolean exists(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.exists(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }


    @Override
    public Long expire(String key, int seconds) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.expire(key, seconds);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long expireAt(String key, long unixTime) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.expireAt(key, unixTime);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long persist(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.persist(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long pexpire(String key, long milliseconds) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.pexpire(key, milliseconds);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long pexpireAt(String key, long unixTime) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.pexpireAt(key, unixTime);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long pttl(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.pttl(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public List<String> sort(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.sort(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long ttl(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.ttl(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String type(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.type(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long append(String key, String value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.append(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long bitcount(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.bitcount(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long bitcount(String key, long start, long end) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.bitcount(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long decr(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.decr(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long decrBy(String key, long value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.decrBy(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String get(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.get(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Boolean getbit(String key, long offset) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.getbit(key, offset);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.getrange(key, startOffset, endOffset);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String getSet(String key, String value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.getSet(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long incr(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.incr(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long incrBy(String key, Integer value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.incrBy(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Double incrByFloat(String key, double value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.incrByFloat(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String psetex(String key, long milliseconds, String value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.psetex(key, milliseconds, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String set(String key, String value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.set(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.setbit(key, offset, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Boolean setbit(String key, long offset, String value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.setbit(key, offset, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String setex(String key, int seconds, String value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.setex(key, seconds, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long setnx(String key, String value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.setnx(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long setrange(String key, long offset, String value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.setrange(key, offset, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long strlen(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.strlen(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long hdel(String key, String[] fields) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.hdel(key, fields);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Boolean hexists(String key, String field) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.hexists(key, field);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String hget(String key, String field) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.hget(key, field);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Map<String, String> hgetAll(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.hgetAll(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long hincrBy(String key, String field, long value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.hincrBy(key, field, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.hincrByFloat(key, field, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Set<String> hkeys(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.hkeys(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long hlen(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.hlen(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public List<String> hmget(String key, String[] fields) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.hmget(key, fields);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String hmset(String key, Map<String, String> hash) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.hmset(key, hash);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long hset(String key, String field, String value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.hset(key, field, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long hsetnx(String key, String field, String value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.hsetnx(key, field, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public List<String> hvals(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.hvals(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public List<String> blpop(int timeout, String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.blpop(timeout, key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public List<String> brpop(int timeout, String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.brpop(timeout, key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String lindex(String key, long index) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.lindex(key, index);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long linsert(String key, ListPosition where, String pivot, String value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.linsert(key, where, pivot, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long llen(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.llen(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String lpop(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.lpop(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long lpush(String key, String... values) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.lpush(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long lpushEx(String key, int seconds, String... values) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            Long num = shardedJedis.lpush(key, values);
            expire(key, seconds);
            return num;
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long lpushx(String key, String... values) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.lpushx(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public List<String> lrange(String key, long start, long end) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.lrange(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long lrem(String key, long count, String value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.lrem(key, count, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String lset(String key, long index, String value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.lset(key, index, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String ltrim(String key, long start, long end) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.ltrim(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String rpop(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.rpop(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long rpush(String key, String... values) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.rpush(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long rpushx(String key, String... values) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.rpushx(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long sadd(String key, String... values) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.sadd(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long scard(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.scard(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Boolean sismember(String key, String value) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.sismember(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Set<String> smembers(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.smembers(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String spop(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.spop(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Set<String> spop(String key, long count) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.spop(key, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public String srandmember(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.srandmember(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public List<String> srandmember(String key, int count) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.srandmember(key, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long srem(String key, String... values) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.srem(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long zadd(String key, double score, String member) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.zadd(key, score, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.zadd(key, scoreMembers);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long zcard(String key) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.zcard(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long zcount(String key, double min, double max) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.zcount(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long zcount(String key, String min, String max) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.zcount(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Double zincrby(String key, double score, String member) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.zincrby(key, score, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Set<String> zrange(String key, long start, long end) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.zrange(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.zrangeByScore(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.zrangeByScore(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.zrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.zrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long zrank(String key, String member) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.zrank(key, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long zrem(String key, String... members) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.zrem(key, members);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long zremrangeByRank(String key, long start, long end) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.zremrangeByRank(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long zremrangeByScore(String key, String start, String end) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.zrevrange(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double min, double max) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.zrevrangeByScore(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String min, String max) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.zrevrangeByScore(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double min, double max, int offset, int count) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.zrevrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String min, String max, int offset, int count) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.zrevrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long zrevrank(String key, String member) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return shardedJedis.zrevrank(key, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Double zscore(String key, String member) throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.zscore(key, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long hsetObject(String key, String field, Object value)
            throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.hset(SafeEncoder.encode(key), SafeEncoder.encode(field), TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @Override
    public Long hsetObjectNx(String key, String field, Object value)
            throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = this.writePool.getResource();
            return shardedJedis.hsetnx(SafeEncoder.encode(key), SafeEncoder.encode(field), TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T hgetObject(String key, String field)
            throws RedisAccessException {
        ShardedJedis shardedJedis = null;
        try {
            if (this.readPool != null) {
                shardedJedis = this.readPool.getResource();
            } else {
                shardedJedis = this.writePool.getResource();
            }
            return (T) TranscoderUtils.decodeObject(shardedJedis.hget(SafeEncoder.encode(key), SafeEncoder.encode(field)));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(shardedJedis);
        }
    }

}
