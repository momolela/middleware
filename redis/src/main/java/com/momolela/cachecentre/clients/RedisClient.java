package com.momolela.cachecentre.clients;

import com.momolela.cachecentre.exception.RedisAccessException;
import redis.clients.jedis.ListPosition;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RedisClient {

    String setObject(String key, Object value) throws RedisAccessException;

    Long hsetObject(String key, String field, Object value) throws RedisAccessException;

    Long setObjectNx(String key, Object value) throws RedisAccessException;

    Long hsetObjectNx(String key, String field, Object value) throws RedisAccessException;

    String setObjectEx(String key, int seconds, Object value) throws RedisAccessException;

    <T> T getObject(String key) throws RedisAccessException;

    <T> T hgetObject(String key, String field) throws RedisAccessException;

    Long delObject(String key) throws RedisAccessException;

    Boolean existsObject(String key) throws RedisAccessException;

    Long del(String key) throws RedisAccessException;

    Boolean exists(String key) throws RedisAccessException;

    Long expire(String key, int seconds) throws RedisAccessException;

    Long expireAt(String key, long unixTime) throws RedisAccessException;

    Long persist(String key) throws RedisAccessException;

    Long pexpire(String key, long milliseconds) throws RedisAccessException;

    Long pexpireAt(String key, long unixTime) throws RedisAccessException;

    Long pttl(String key) throws RedisAccessException;

    List<String> sort(String key) throws RedisAccessException;

    Long ttl(String key) throws RedisAccessException;

    String type(String key) throws RedisAccessException;

    Long append(String key, String value) throws RedisAccessException;

    Long bitcount(String key) throws RedisAccessException;

    Long bitcount(String key, long start, long end) throws RedisAccessException;

    Long decr(String key) throws RedisAccessException;

    Long decrBy(String key, long value) throws RedisAccessException;

    String get(String key) throws RedisAccessException;

    Boolean getbit(String key, long offset) throws RedisAccessException;

    String getrange(String key, long startOffset, long endOffset) throws RedisAccessException;

    String getSet(String key, String value) throws RedisAccessException;

    Long incr(String key) throws RedisAccessException;

    Long incrBy(String key, Integer value) throws RedisAccessException;

    Double incrByFloat(String key, double value) throws RedisAccessException;

    String psetex(String key, long milliseconds, String value) throws RedisAccessException;

    String set(String key, String value) throws RedisAccessException;

    Boolean setbit(String key, long offset, boolean value) throws RedisAccessException;

    Boolean setbit(String key, long offset, String value) throws RedisAccessException;

    String setex(String key, int seconds, String value) throws RedisAccessException;

    Long setnx(String key, String value) throws RedisAccessException;

    Long setrange(String key, long offset, String value) throws RedisAccessException;

    Long strlen(String key) throws RedisAccessException;

    Long hdel(String key, String... fields) throws RedisAccessException;

    Boolean hexists(String key, String field) throws RedisAccessException;

    String hget(String key, String field) throws RedisAccessException;

    Map<String, String> hgetAll(String key) throws RedisAccessException;

    Long hincrBy(String key, String field, long value) throws RedisAccessException;

    Double hincrByFloat(byte[] key, byte[] field, double value) throws RedisAccessException;

    Set<String> hkeys(String key) throws RedisAccessException;

    Long hlen(String key) throws RedisAccessException;

    List<String> hmget(String key, String... fields) throws RedisAccessException;

    String hmset(String key, Map<String, String> hash) throws RedisAccessException;

    Long hset(String key, String field, String value) throws RedisAccessException;

    Long hsetnx(String key, String field, String value) throws RedisAccessException;

    List<String> hvals(String key) throws RedisAccessException;

    List<String> blpop(int timeout, String key) throws RedisAccessException;

    List<String> brpop(int timeout, String key) throws RedisAccessException;

    String lindex(String key, long index) throws RedisAccessException;

    Long linsert(String key, ListPosition where, String pivot, String value) throws RedisAccessException;

    Long llen(String key) throws RedisAccessException;

    String lpop(String key) throws RedisAccessException;

    Long lpush(String key, String... values) throws RedisAccessException;

    Long lpushEx(String key, int seconds, String... values) throws RedisAccessException;

    Long lpushx(String key, String... values) throws RedisAccessException;

    List<String> lrange(String key, long start, long end) throws RedisAccessException;

    Long lrem(String key, long count, String value) throws RedisAccessException;

    String lset(String key, long index, String value) throws RedisAccessException;

    String ltrim(String key, long start, long end) throws RedisAccessException;

    String rpop(String key) throws RedisAccessException;

    Long rpush(String key, String... values) throws RedisAccessException;

    Long rpushx(String key, String... values) throws RedisAccessException;

    Long sadd(String key, String... values) throws RedisAccessException;

    Long scard(String key) throws RedisAccessException;

    Boolean sismember(String key, String value) throws RedisAccessException;

    Set<String> smembers(String key) throws RedisAccessException;

    String spop(String key) throws RedisAccessException;

    Set<String> spop(String key, long count) throws RedisAccessException;

    String srandmember(String key) throws RedisAccessException;

    List<String> srandmember(String key, int count) throws RedisAccessException;

    Long srem(String key, String... values) throws RedisAccessException;

    Long zadd(String key, double score, String member) throws RedisAccessException;

    Long zadd(String key, Map<String, Double> scoreMembers) throws RedisAccessException;

    Long zcard(String key) throws RedisAccessException;

    Long zcount(String key, double min, double max) throws RedisAccessException;

    Long zcount(String key, String min, String max) throws RedisAccessException;

    Double zincrby(String key, double score, String member) throws RedisAccessException;

    Set<String> zrange(String key, long start, long end) throws RedisAccessException;

    Set<String> zrangeByScore(String key, double min, double max) throws RedisAccessException;

    Set<String> zrangeByScore(String key, String min, String max) throws RedisAccessException;

    Set<String> zrangeByScore(String key, double min, double max, int offset, int count) throws RedisAccessException;

    Set<String> zrangeByScore(String key, String min, String max, int offset, int count) throws RedisAccessException;

    Long zrank(String key, String member) throws RedisAccessException;

    Long zrem(String key, String... members) throws RedisAccessException;

    Long zremrangeByRank(String key, long start, long end) throws RedisAccessException;

    Long zremrangeByScore(String key, double start, double end) throws RedisAccessException;

    Long zremrangeByScore(String key, String start, String end) throws RedisAccessException;

    Set<String> zrevrange(String key, long start, long end) throws RedisAccessException;

    Set<String> zrevrangeByScore(String key, double min, double max) throws RedisAccessException;

    Set<String> zrevrangeByScore(String key, String min, String max) throws RedisAccessException;

    Set<String> zrevrangeByScore(String key, double min, double max, int offset, int count) throws RedisAccessException;

    Set<String> zrevrangeByScore(String key, String min, String max, int offset, int count) throws RedisAccessException;

    Long zrevrank(String key, String member) throws RedisAccessException;

    Double zscore(String key, String member) throws RedisAccessException;
}
