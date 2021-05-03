package com.momolela.cachecentre.clients.single;

import com.momolela.cachecentre.clients.RedisClient;
import com.momolela.cachecentre.config.single.RedisSingleClientConfig;
import com.momolela.cachecentre.exception.RedisAccessException;
import com.momolela.cachecentre.exception.RedisInitializerException;
import com.momolela.cachecentre.utils.CloseUtil;
import com.momolela.cachecentre.utils.TranscoderUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ListPosition;
import redis.clients.jedis.util.SafeEncoder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisSingleClient implements RedisClient {

    private static final Log LOG = LogFactory.getLog(RedisSingleClient.class);
    private JedisPool jedisPool;
    private RedisSingleClientConfig redisClientConfig; // redis连接配置
    private JedisPoolConfig jedisPoolConfig; // redis连接池配置

    /**
     * 加载配置构造器
     *
     * @param redisClientConfig
     */
    public RedisSingleClient(RedisSingleClientConfig redisClientConfig) {
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
        LOG.info("redis single client init");
        String ipPort = this.redisClientConfig.getServerConfString();
        if (StringUtils.isNotBlank(ipPort)) {
            String[] ipPortArray = ipPort.split(":");
            if (ipPortArray.length == 1) {
                throw new RedisInitializerException(Arrays.toString(ipPortArray) + " is not include host:port or host:port:passwd after split \":\"");
            } else {
                String ip = ipPortArray[0];
                int port = Integer.valueOf(ipPortArray[1]);
                jedisPool = new JedisPool(jedisPoolConfig, ip, port, this.redisClientConfig.getConnectionTimeout(),
                        this.redisClientConfig.getSoTimeout(), this.redisClientConfig.getPassword(), this.redisClientConfig.getDbIndex(),
                        null, false, null, null, null);
                LOG.info("redis client connect ip:" + ip + ",port:" + port);
            }
        }
    }


    @Override
    public String setObject(String key, Object value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.set(SafeEncoder.encode(key), TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long setObjectNx(String key, Object value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.setnx(SafeEncoder.encode(key), TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String setObjectEx(String key, int seconds, Object value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.setex(SafeEncoder.encode(key), seconds, TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public <T> T getObject(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return (T) TranscoderUtils.decodeObject(jedis.get(SafeEncoder.encode(key)));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long delObject(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.del(SafeEncoder.encode(key));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Boolean existsObject(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.exists(SafeEncoder.encode(key));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long del(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.del(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Boolean exists(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.exists(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long expire(String key, int seconds) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.expire(key, seconds);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long expireAt(String key, long unixTime) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.expireAt(key, unixTime);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long persist(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.persist(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long pexpire(String key, long milliseconds) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.pexpire(key, milliseconds);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long pexpireAt(String key, long unixTime) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.pexpireAt(key, unixTime);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long pttl(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.pttl(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public List<String> sort(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.sort(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long ttl(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.ttl(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String type(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.type(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long append(String key, String value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.append(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long bitcount(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.bitcount(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long bitcount(String key, long start, long end) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.bitcount(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long decr(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.decr(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long decrBy(String key, long value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.decrBy(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String get(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.get(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Boolean getbit(String key, long offset) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.getbit(key, offset);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.getrange(key, startOffset, endOffset);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String getSet(String key, String value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.getSet(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long incr(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.incr(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long incrBy(String key, Integer value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.incrBy(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Double incrByFloat(String key, double value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.incrByFloat(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String psetex(String key, long milliseconds, String value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.psetex(key, milliseconds, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String set(String key, String value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.set(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.setbit(key, offset, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Boolean setbit(String key, long offset, String value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.setbit(key, offset, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String setex(String key, int seconds, String value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.setex(key, seconds, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long setnx(String key, String value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.setnx(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long setrange(String key, long offset, String value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.setrange(key, offset, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long strlen(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.strlen(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long hdel(String key, String... fields) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hdel(key, fields);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Boolean hexists(String key, String field) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hexists(key, field);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String hget(String key, String field) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hget(key, field);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Map<String, String> hgetAll(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hgetAll(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long hincrBy(String key, String field, long value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hincrBy(key, field, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hincrByFloat(key, field, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Set<String> hkeys(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hkeys(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long hlen(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hlen(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public List<String> hmget(String key, String... fields) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hmget(key, fields);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String hmset(String key, Map<String, String> hash) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hmset(key, hash);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long hset(String key, String field, String value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hset(key, field, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long hsetnx(String key, String field, String value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hsetnx(key, field, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public List<String> hvals(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hvals(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public List<String> blpop(int timeout, String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.blpop(timeout, key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public List<String> brpop(int timeout, String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.brpop(timeout, key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String lindex(String key, long index) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.lindex(key, index);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long linsert(String key, ListPosition where, String pivot, String value) throws
            RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.linsert(key, where, pivot, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long llen(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.llen(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String lpop(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.lpop(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long lpush(String key, String... values) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.lpush(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long lpushEx(String key, int seconds, String... values) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long num = jedis.lpush(key, values);
            expire(key, seconds);
            return num;
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long lpushx(String key, String... values) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.lpushx(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public List<String> lrange(String key, long start, long end) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.lrange(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long lrem(String key, long count, String value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.lrem(key, count, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String lset(String key, long index, String value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.lset(key, index, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String ltrim(String key, long start, long end) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.ltrim(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String rpop(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.rpop(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long rpush(String key, String... values) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.rpush(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long rpushx(String key, String... values) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.rpushx(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long sadd(String key, String... values) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.sadd(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long scard(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.scard(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Boolean sismember(String key, String value) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.sismember(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Set<String> smembers(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.smembers(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String spop(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.spop(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Set<String> spop(String key, long count) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.spop(key, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public String srandmember(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.srandmember(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public List<String> srandmember(String key, int count) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.srandmember(key, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long srem(String key, String... values) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.srem(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long zadd(String key, double score, String member) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zadd(key, score, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zadd(key, scoreMembers);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long zcard(String key) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zcard(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long zcount(String key, double min, double max) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zcount(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long zcount(String key, String min, String max) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zcount(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Double zincrby(String key, double score, String member) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zincrby(key, score, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Set<String> zrange(String key, long start, long end) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrange(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrangeByScore(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrangeByScore(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) throws
            RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) throws
            RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long zrank(String key, String member) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrank(key, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long zrem(String key, String... members) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrem(key, members);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long zremrangeByRank(String key, long start, long end) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zremrangeByRank(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long zremrangeByScore(String key, String start, String end) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrange(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double min, double max) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrangeByScore(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String min, String max) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrangeByScore(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double min, double max, int offset, int count) throws
            RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String min, String max, int offset, int count) throws
            RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long zrevrank(String key, String member) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrank(key, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Double zscore(String key, String member) throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zscore(key, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long hsetObject(String key, String field, Object value)
            throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hset(SafeEncoder.encode(key), SafeEncoder.encode(field), TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @Override
    public Long hsetObjectNx(String key, String field, Object value)
            throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hsetnx(SafeEncoder.encode(key), SafeEncoder.encode(field), TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T hgetObject(String key, String field)
            throws RedisAccessException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return (T) TranscoderUtils.decodeObject(jedis.hget(SafeEncoder.encode(key), SafeEncoder.encode(field)));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            CloseUtil.close(jedis);
        }
    }


}
