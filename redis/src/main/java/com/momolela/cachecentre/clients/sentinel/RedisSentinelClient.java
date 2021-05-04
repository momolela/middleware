package com.momolela.cachecentre.clients.sentinel;

import com.momolela.cachecentre.clients.RedisClient;
import com.momolela.cachecentre.config.sentinel.RedisSentinelClientConfig;
import com.momolela.cachecentre.exception.RedisAccessException;
import com.momolela.cachecentre.utils.CloseUtil;
import com.momolela.cachecentre.utils.TranscoderUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.ListPosition;
import redis.clients.jedis.util.SafeEncoder;

import java.util.*;

public class RedisSentinelClient implements RedisClient {

    private static final Log LOG = LogFactory.getLog(RedisSentinelClient.class);
    private JedisSentinelPool jedisSentinelPool;
    private RedisSentinelClientConfig redisClientConfig; // redis连接配置
    private GenericObjectPoolConfig genericObjectPoolConfig; // apache连接池

    /**
     * 加载配置构造器
     *
     * @param redisClientConfig
     */
    public RedisSentinelClient(RedisSentinelClientConfig redisClientConfig) {
        this.redisClientConfig = redisClientConfig;
        genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxTotal(redisClientConfig.getMaxTotal());
        genericObjectPoolConfig.setMaxIdle(redisClientConfig.getMaxIdle());
        genericObjectPoolConfig.setMinIdle(redisClientConfig.getMinIdle());
        genericObjectPoolConfig.setMaxWaitMillis(redisClientConfig.getMaxWaitMillis());
        genericObjectPoolConfig.setMinEvictableIdleTimeMillis(redisClientConfig.getMinEvictableIdleTimeMillis());
        genericObjectPoolConfig.setNumTestsPerEvictionRun(redisClientConfig.getNumTestsPerEvictionRun());
        genericObjectPoolConfig.setTimeBetweenEvictionRunsMillis(redisClientConfig.getTimeBetweenEvictionRunsMillis());
        genericObjectPoolConfig.setTestOnBorrow(redisClientConfig.isTestOnBorrow());
        genericObjectPoolConfig.setTestWhileIdle(redisClientConfig.isTestWhileIdle());
        init();
    }

    /**
     * 初始化redis连接
     */
    private void init() {
        LOG.info("redis sentinel client init");
        List<String> ipsConfList = Arrays.asList(redisClientConfig.getServerConfString().split(","));
        Set<String> set = new HashSet<String>();
        for (String ipsConf : ipsConfList) {
            set.add(ipsConf);
        }
        jedisSentinelPool = new JedisSentinelPool(redisClientConfig.getMasterName(), set, genericObjectPoolConfig, redisClientConfig.getConnectionTimeout(), redisClientConfig.getSoTimeout(), redisClientConfig.getPassword(), redisClientConfig.getDbIndex());
    }

    private Jedis getJedisSentinel() {
        return jedisSentinelPool.getResource();
    }

    @Override
    public String setObject(String key, Object value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.set(SafeEncoder.encode(key), TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long setObjectNx(String key, Object value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.setnx(SafeEncoder.encode(key), TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String setObjectEx(String key, int seconds, Object value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.setex(SafeEncoder.encode(key), seconds, TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getObject(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return (T) TranscoderUtils.decodeObject(jedisSentinel.get(SafeEncoder.encode(key)));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long delObject(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.del(SafeEncoder.encode(key));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Boolean existsObject(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.exists(SafeEncoder.encode(key));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long del(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.del(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Boolean exists(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.exists(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long expire(String key, int seconds) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.expire(key, seconds);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long expireAt(String key, long unixTime) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.expireAt(key, unixTime);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long persist(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.persist(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long pexpire(String key, long milliseconds) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.pexpire(key, milliseconds);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long pexpireAt(String key, long unixTime) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.pexpireAt(key, unixTime);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long pttl(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.pttl(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public List<String> sort(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.sort(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long ttl(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.ttl(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String type(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.type(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long append(String key, String value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.append(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long bitcount(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.bitcount(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long bitcount(String key, long start, long end) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.bitcount(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long decr(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.decr(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long decrBy(String key, long value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.decrBy(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String get(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.get(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Boolean getbit(String key, long offset) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.getbit(key, offset);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.getrange(key, startOffset, endOffset);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String getSet(String key, String value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.getSet(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long incr(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.incr(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long incrBy(String key, Integer value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.incrBy(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Double incrByFloat(String key, double value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.incrByFloat(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String psetex(String key, long milliseconds, String value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.psetex(key, milliseconds, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String set(String key, String value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.set(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.setbit(key, offset, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Boolean setbit(String key, long offset, String value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.setbit(key, offset, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String setex(String key, int seconds, String value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.setex(key, seconds, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long setnx(String key, String value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.setnx(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long setrange(String key, long offset, String value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.setrange(key, offset, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long strlen(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.strlen(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long hdel(String key, String... fields) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.hdel(key, fields);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Boolean hexists(String key, String field) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.hexists(key, field);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String hget(String key, String field) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.hget(key, field);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Map<String, String> hgetAll(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.hgetAll(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long hincrBy(String key, String field, long value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.hincrBy(key, field, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.hincrByFloat(key, field, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Set<String> hkeys(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.hkeys(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long hlen(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.hlen(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public List<String> hmget(String key, String... fields) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.hmget(key, fields);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String hmset(String key, Map<String, String> hash) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.hmset(key, hash);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long hset(String key, String field, String value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.hset(key, field, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long hsetnx(String key, String field, String value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.hsetnx(key, field, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public List<String> hvals(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.hvals(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public List<String> blpop(int timeout, String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.blpop(timeout, key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public List<String> brpop(int timeout, String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.brpop(timeout, key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String lindex(String key, long index) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.lindex(key, index);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long linsert(String key, ListPosition where, String pivot, String value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.linsert(key, where, pivot, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long llen(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.llen(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String lpop(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.lpop(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long lpush(String key, String... values) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.lpush(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long lpushEx(String key, int seconds, String... values) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            Long num = jedisSentinel.lpush(key, values);
            expire(key, seconds);
            return num;
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long lpushx(String key, String... values) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.lpushx(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public List<String> lrange(String key, long start, long end) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.lrange(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long lrem(String key, long count, String value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.lrem(key, count, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String lset(String key, long index, String value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.lset(key, index, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String ltrim(String key, long start, long end) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.ltrim(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String rpop(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.rpop(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long rpush(String key, String... values) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.rpush(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long rpushx(String key, String... values) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.rpushx(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long sadd(String key, String... values) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.sadd(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long scard(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.scard(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Boolean sismember(String key, String value) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.sismember(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Set<String> smembers(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.smembers(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String spop(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.spop(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Set<String> spop(String key, long count) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.spop(key, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public String srandmember(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.srandmember(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public List<String> srandmember(String key, int count) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.srandmember(key, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long srem(String key, String... values) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.srem(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long zadd(String key, double score, String member) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zadd(key, score, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zadd(key, scoreMembers);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long zcard(String key) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zcard(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long zcount(String key, double min, double max) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zcount(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long zcount(String key, String min, String max) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zcount(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Double zincrby(String key, double score, String member) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zincrby(key, score, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Set<String> zrange(String key, long start, long end) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zrange(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zrangeByScore(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zrangeByScore(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long zrank(String key, String member) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zrank(key, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long zrem(String key, String... members) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zrem(key, members);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long zremrangeByRank(String key, long start, long end) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zremrangeByRank(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long zremrangeByScore(String key, String start, String end) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zrevrange(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double min, double max) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zrevrangeByScore(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String min, String max) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zrevrangeByScore(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double min, double max, int offset, int count) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zrevrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String min, String max, int offset, int count) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zrevrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long zrevrank(String key, String member) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zrevrank(key, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Double zscore(String key, String member) throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.zscore(key, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long hsetObject(String key, String field, Object value)
            throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.hset(SafeEncoder.encode(key), SafeEncoder.encode(field), TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {

                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @Override
    public Long hsetObjectNx(String key, String field, Object value)
            throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return jedisSentinel.hsetnx(SafeEncoder.encode(key), SafeEncoder.encode(field), TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T hgetObject(String key, String field)
            throws RedisAccessException {
        Jedis jedisSentinel = null;
        try {
            jedisSentinel = getJedisSentinel();
            return (T) TranscoderUtils.decodeObject(jedisSentinel.hget(SafeEncoder.encode(key), SafeEncoder.encode(field)));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        } finally {
            if (jedisSentinel != null) {
                CloseUtil.close(jedisSentinel);
            }
        }
    }
}
