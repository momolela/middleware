package com.momolela.cachecentre.clients.cluster;

import com.momolela.cachecentre.clients.RedisClient;
import com.momolela.cachecentre.config.cluster.RedisClusterClientConfig;
import com.momolela.cachecentre.exception.RedisAccessException;
import com.momolela.cachecentre.exception.RedisInitializerException;
import com.momolela.cachecentre.utils.TranscoderUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ListPosition;
import redis.clients.jedis.util.SafeEncoder;

import java.util.*;

public class RedisClusterClient implements RedisClient {

    private static final Log LOG = LogFactory.getLog(RedisClusterClient.class);
    private RedisClusterClientConfig redisClientConfig; // redis连接配置
    private JedisCluster jedisCluster; // redis-cluster 操作类
    private GenericObjectPoolConfig genericObjectPoolConfig; // apache 连接池

    /**
     * 加载配置构造器
     *
     * @param redisClientConfig
     */
    public RedisClusterClient(RedisClusterClientConfig redisClientConfig) {
        this.redisClientConfig = redisClientConfig;
        genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxWaitMillis(redisClientConfig.getMaxWaitMillis());
        genericObjectPoolConfig.setMaxTotal(redisClientConfig.getMaxTotal());
        genericObjectPoolConfig.setMaxIdle(redisClientConfig.getMaxIdle());
        genericObjectPoolConfig.setMinIdle(redisClientConfig.getMinIdle());
        init();
    }

    /**
     * 初始化redis连接
     */
    private void init() {
        LOG.info("redis cluster client init");
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        List<String> ipsConfList = Arrays.asList(this.redisClientConfig.getClusterConfString().split("(?:\\s|,)+"));
        Set<HostAndPort> hostAndPortSet = new HashSet<>();
        for (String ipPort : ipsConfList) {
            if (StringUtils.isNotBlank(ipPort)) {
                String[] ipPortArray = ipPort.split(":");
                if (ipPortArray.length == 1) {
                    throw new RedisInitializerException(Arrays.toString(ipPortArray) + " is not include host:port or host:port:pass after split \":\"");
                } else {
                    String ip = ipPortArray[0];
                    int port = Integer.valueOf(ipPortArray[1]);
                    hostAndPortSet.add(new HostAndPort(ip, port));
                    LOG.info("redis client connect ip:" + ip + ",port:" + port);
                }
            }
        }
        jedisCluster = new JedisCluster(hostAndPortSet, redisClientConfig.getConnectionTimeout(), redisClientConfig.getSoTimeout(), redisClientConfig.getMaxRedirects(), redisClientConfig.getPassword(), this.genericObjectPoolConfig);
    }

    @Override
    public String setObject(String key, Object value) throws RedisAccessException {
        try {
            return jedisCluster.set(SafeEncoder.encode(key), TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long setObjectNx(String key, Object value) throws RedisAccessException {
        try {
            return jedisCluster.setnx(SafeEncoder.encode(key), TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String setObjectEx(String key, int seconds, Object value) throws RedisAccessException {
        try {
            return jedisCluster.setex(SafeEncoder.encode(key), seconds, TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public <T> T getObject(String key) throws RedisAccessException {
        try {
            return (T) TranscoderUtils.decodeObject(jedisCluster.get(SafeEncoder.encode(key)));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long delObject(String key) throws RedisAccessException {
        try {
            return jedisCluster.del(SafeEncoder.encode(key));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Boolean existsObject(String key) throws RedisAccessException {
        try {
            return jedisCluster.exists(SafeEncoder.encode(key));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long del(String key) throws RedisAccessException {
        try {
            return jedisCluster.del(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Boolean exists(String key) throws RedisAccessException {
        try {
            return jedisCluster.exists(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long expire(String key, int seconds) throws RedisAccessException {
        try {
            return jedisCluster.expire(key, seconds);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long expireAt(String key, long unixTime) throws RedisAccessException {
        try {
            return jedisCluster.expireAt(key, unixTime);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long persist(String key) throws RedisAccessException {
        try {
            return jedisCluster.persist(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long pexpire(String key, long milliseconds) throws RedisAccessException {
        try {
            return jedisCluster.pexpire(key, milliseconds);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long pexpireAt(String key, long unixTime) throws RedisAccessException {
        try {
            return jedisCluster.pexpireAt(key, unixTime);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long pttl(String key) throws RedisAccessException {
        try {
            return jedisCluster.pttl(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public List<String> sort(String key) throws RedisAccessException {
        try {
            return jedisCluster.sort(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long ttl(String key) throws RedisAccessException {
        try {
            return jedisCluster.ttl(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String type(String key) throws RedisAccessException {
        try {
            return jedisCluster.type(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long append(String key, String value) throws RedisAccessException {
        try {
            return jedisCluster.append(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long bitcount(String key) throws RedisAccessException {
        try {
            return jedisCluster.bitcount(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long bitcount(String key, long start, long end) throws RedisAccessException {
        try {
            return jedisCluster.bitcount(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long decr(String key) throws RedisAccessException {
        try {
            return jedisCluster.decr(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long decrBy(String key, long value) throws RedisAccessException {
        try {
            return jedisCluster.decrBy(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String get(String key) throws RedisAccessException {
        try {
            return jedisCluster.get(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Boolean getbit(String key, long offset) throws RedisAccessException {
        try {
            return jedisCluster.getbit(key, offset);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) throws RedisAccessException {
        try {
            return jedisCluster.getrange(key, startOffset, endOffset);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String getSet(String key, String value) throws RedisAccessException {
        try {
            return jedisCluster.getSet(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long incr(String key) throws RedisAccessException {
        try {
            return jedisCluster.incr(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long incrBy(String key, Integer value) throws RedisAccessException {
        try {
            return jedisCluster.incrBy(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Double incrByFloat(String key, double value) throws RedisAccessException {
        try {
            return jedisCluster.incrByFloat(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String psetex(String key, long milliseconds, String value) throws RedisAccessException {
        try {
            return jedisCluster.psetex(key, milliseconds, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String set(String key, String value) throws RedisAccessException {
        try {
            return jedisCluster.set(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) throws RedisAccessException {
        try {
            return jedisCluster.setbit(key, offset, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Boolean setbit(String key, long offset, String value) throws RedisAccessException {
        try {
            return jedisCluster.setbit(key, offset, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String setex(String key, int seconds, String value) throws RedisAccessException {
        try {
            return jedisCluster.setex(key, seconds, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long setnx(String key, String value) throws RedisAccessException {
        try {
            return jedisCluster.setnx(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long setrange(String key, long offset, String value) throws RedisAccessException {
        try {
            return jedisCluster.setrange(key, offset, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long strlen(String key) throws RedisAccessException {
        try {
            return jedisCluster.strlen(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long hdel(String key, String... fields) throws RedisAccessException {
        try {
            return jedisCluster.hdel(key, fields);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Boolean hexists(String key, String field) throws RedisAccessException {
        try {
            return jedisCluster.hexists(key, field);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String hget(String key, String field) throws RedisAccessException {
        try {
            return jedisCluster.hget(key, field);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Map<String, String> hgetAll(String key) throws RedisAccessException {
        try {
            return jedisCluster.hgetAll(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long hincrBy(String key, String field, long value) throws RedisAccessException {
        try {
            return jedisCluster.hincrBy(key, field, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double value) throws RedisAccessException {
        try {
            return jedisCluster.hincrByFloat(key, field, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Set<String> hkeys(String key) throws RedisAccessException {
        try {
            return jedisCluster.hkeys(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long hlen(String key) throws RedisAccessException {
        try {
            return jedisCluster.hlen(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public List<String> hmget(String key, String... fields) throws RedisAccessException {
        try {
            return jedisCluster.hmget(key, fields);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String hmset(String key, Map<String, String> hash) throws RedisAccessException {
        try {
            return jedisCluster.hmset(key, hash);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long hset(String key, String field, String value) throws RedisAccessException {
        try {
            return jedisCluster.hset(key, field, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long hsetnx(String key, String field, String value) throws RedisAccessException {
        try {
            return jedisCluster.hsetnx(key, field, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public List<String> hvals(String key) throws RedisAccessException {
        try {
            return jedisCluster.hvals(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public List<String> blpop(int timeout, String key) throws RedisAccessException {
        try {
            return jedisCluster.blpop(timeout, key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public List<String> brpop(int timeout, String key) throws RedisAccessException {
        try {
            return jedisCluster.brpop(timeout, key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String lindex(String key, long index) throws RedisAccessException {
        try {
            return jedisCluster.lindex(key, index);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long linsert(String key, ListPosition where, String pivot, String value) throws RedisAccessException {
        try {
            return jedisCluster.linsert(key, where, pivot, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long llen(String key) throws RedisAccessException {
        try {
            return jedisCluster.llen(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String lpop(String key) throws RedisAccessException {
        try {
            return jedisCluster.lpop(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long lpush(String key, String... values) throws RedisAccessException {
        try {
            return jedisCluster.lpush(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long lpushEx(String key, int seconds, String... values) throws RedisAccessException {
        try {
            Long num = jedisCluster.lpush(key, values);
            expire(key, seconds);
            return num;
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long lpushx(String key, String... values) throws RedisAccessException {
        try {
            return jedisCluster.lpushx(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public List<String> lrange(String key, long start, long end) throws RedisAccessException {
        try {
            return jedisCluster.lrange(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long lrem(String key, long count, String value) throws RedisAccessException {
        try {
            return jedisCluster.lrem(key, count, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String lset(String key, long index, String value) throws RedisAccessException {
        try {
            return jedisCluster.lset(key, index, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String ltrim(String key, long start, long end) throws RedisAccessException {
        try {
            return jedisCluster.ltrim(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String rpop(String key) throws RedisAccessException {
        try {
            return jedisCluster.rpop(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long rpush(String key, String... values) throws RedisAccessException {
        try {
            return jedisCluster.rpush(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long rpushx(String key, String... values) throws RedisAccessException {
        try {
            return jedisCluster.rpushx(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long sadd(String key, String... values) throws RedisAccessException {
        try {
            return jedisCluster.sadd(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long scard(String key) throws RedisAccessException {
        try {
            return jedisCluster.scard(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Boolean sismember(String key, String value) throws RedisAccessException {
        try {
            return jedisCluster.sismember(key, value);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Set<String> smembers(String key) throws RedisAccessException {
        try {
            return jedisCluster.smembers(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String spop(String key) throws RedisAccessException {
        try {
            return jedisCluster.spop(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Set<String> spop(String key, long count) throws RedisAccessException {
        try {
            return jedisCluster.spop(key, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public String srandmember(String key) throws RedisAccessException {
        try {
            return jedisCluster.srandmember(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public List<String> srandmember(String key, int count) throws RedisAccessException {
        try {
            return jedisCluster.srandmember(key, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long srem(String key, String... values) throws RedisAccessException {
        try {
            return jedisCluster.srem(key, values);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long zadd(String key, double score, String member) throws RedisAccessException {
        try {
            return jedisCluster.zadd(key, score, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) throws RedisAccessException {
        try {
            return jedisCluster.zadd(key, scoreMembers);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long zcard(String key) throws RedisAccessException {
        try {
            return jedisCluster.zcard(key);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long zcount(String key, double min, double max) throws RedisAccessException {
        try {
            return jedisCluster.zcount(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long zcount(String key, String min, String max) throws RedisAccessException {
        try {
            return jedisCluster.zcount(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Double zincrby(String key, double score, String member) throws RedisAccessException {
        try {
            return jedisCluster.zincrby(key, score, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Set<String> zrange(String key, long start, long end) throws RedisAccessException {
        try {
            return jedisCluster.zrange(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) throws RedisAccessException {
        try {
            return jedisCluster.zrangeByScore(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) throws RedisAccessException {
        try {
            return jedisCluster.zrangeByScore(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) throws RedisAccessException {
        try {
            return jedisCluster.zrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) throws RedisAccessException {
        try {
            return jedisCluster.zrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long zrank(String key, String member) throws RedisAccessException {
        try {
            return jedisCluster.zrank(key, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long zrem(String key, String... members) throws RedisAccessException {
        try {
            return jedisCluster.zrem(key, members);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long zremrangeByRank(String key, long start, long end) throws RedisAccessException {
        try {
            return jedisCluster.zremrangeByRank(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) throws RedisAccessException {
        try {
            return jedisCluster.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long zremrangeByScore(String key, String start, String end) throws RedisAccessException {
        try {
            return jedisCluster.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end) throws RedisAccessException {
        try {
            return jedisCluster.zrevrange(key, start, end);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double min, double max) throws RedisAccessException {
        try {
            return jedisCluster.zrevrangeByScore(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String min, String max) throws RedisAccessException {
        try {
            return jedisCluster.zrevrangeByScore(key, min, max);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double min, double max, int offset, int count) throws RedisAccessException {
        try {
            return jedisCluster.zrevrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String min, String max, int offset, int count) throws RedisAccessException {
        try {
            return jedisCluster.zrevrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long zrevrank(String key, String member) throws RedisAccessException {
        try {
            return jedisCluster.zrevrank(key, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Double zscore(String key, String member) throws RedisAccessException {
        try {
            return jedisCluster.zscore(key, member);
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long hsetObject(String key, String field, Object value)
            throws RedisAccessException {
        try {
            return jedisCluster.hset(SafeEncoder.encode(key), SafeEncoder.encode(field), TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @Override
    public Long hsetObjectNx(String key, String field, Object value)
            throws RedisAccessException {
        try {
            return jedisCluster.hsetnx(SafeEncoder.encode(key), SafeEncoder.encode(field), TranscoderUtils.encodeObject(value));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T hgetObject(String key, String field)
            throws RedisAccessException {
        try {
            return (T) TranscoderUtils.decodeObject(jedisCluster.hget(SafeEncoder.encode(key), SafeEncoder.encode(field)));
        } catch (Exception e) {
            throw new RedisAccessException(e);
        }
    }
}
