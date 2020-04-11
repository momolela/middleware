package com.momolela.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.BoundSetOperations;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

public class RedisCacheUtil<T> {
	@SuppressWarnings("rawtypes")
	@Autowired
	private RedisTemplate redisTemplate;

	/**
	 * 缓存基本的对象，Integer、String、实体类等
	 * @param key 缓存的键值
	 * @param value 缓存的值
	 * @return 缓存的对象
	 */
	@SuppressWarnings({ "unchecked", "hiding" })
	public <T> ValueOperations<String, T> setCacheObject(String key, T value) {
		ValueOperations<String, T> operation = redisTemplate.opsForValue();
		operation.set(key, value);
		return operation;
	}

	/** 
	 * 获得缓存的基本对象。 
	 * @param key 缓存键值 
	 * @return 缓存键值对应的数据 
	 */
	@SuppressWarnings({ "unchecked", "hiding" })
	public <T> T getCacheObject(String key) {
		ValueOperations<String, T> operation = redisTemplate.opsForValue();
		return operation.get(key);
	}

	/**
	 * 缓存List数据
	 * @param key 缓存的键值
	 * @param dataList 待缓存的List数据
	 * @return 缓存的对象
	 */
	@SuppressWarnings({ "unchecked", "rawtypes", "hiding" })
	public <T> ListOperations<String, T> setCacheList(String key, List<T> dataList) {
		ListOperations listOperation = redisTemplate.opsForList();
		if (null != dataList) {
			int size = dataList.size();
			for (int i = 0; i < size; i++) {
				listOperation.rightPush(key, dataList.get(i));
			}
		}
		return listOperation;
	}

	/** 
	 * 获得缓存的list对象
	 * @param key 缓存的键值 
	 * @return 缓存键值对应的数据 
	 */
	@SuppressWarnings({ "unchecked", "hiding" })
	public <T> List<T> getCacheList(String key) {
		List<T> dataList = new ArrayList<T>();
		ListOperations<String, T> listOperation = redisTemplate.opsForList();
		Long size = listOperation.size(key);
		for (int i = 0; i < size; i++) {
			dataList.add((T) listOperation.leftPop(key));
		}
		return dataList;
	}

	/**
	 * 缓存Set
	 * @param key 缓存键值
	 * @param dataSet 缓存的数据
	 * @return 缓存数据的对象
	 */
	@SuppressWarnings({ "unchecked", "hiding" })
	public <T> BoundSetOperations<String, T> setCacheSet(String key, Set<T> dataSet) {
		BoundSetOperations<String, T> setOperation = redisTemplate.boundSetOps(key);
		Iterator<T> it = dataSet.iterator();
		while (it.hasNext()) {
			setOperation.add(it.next());
		}
		return setOperation;
	}

	/**
	 * 获得缓存的set
	 * @param key
	 * @return 
	 */
	@SuppressWarnings("unchecked")
	public Set<T> getCacheSet(String key) {
		Set<T> dataSet = new HashSet<T>();
		BoundSetOperations<String, T> operation = redisTemplate.boundSetOps(key);
		Long size = operation.size();
		for (int i = 0; i < size; i++) {
			dataSet.add(operation.pop());
		}
		return dataSet;
	}

	/**
	 * 缓存Map
	 * @param key
	 * @param dataMap
	 * @return 
	 */
	@SuppressWarnings({ "rawtypes", "unchecked", "hiding" })
	public <T> HashOperations<String, String, T> setCacheMap(String key, Map<String, T> dataMap) {
		HashOperations hashOperations = redisTemplate.opsForHash();
		if (null != dataMap) {
			for (Map.Entry<String, T> entry : dataMap.entrySet()) {
				hashOperations.put(key, entry.getKey(), entry.getValue());
			}
		}
		return hashOperations;
	}

	/**
	 * 获得缓存的Map
	 * @param key
	 * @param hashOperation
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "hiding" })
	public <T> Map<String, T> getCacheMap(String key) {
		Map<String, T> map = redisTemplate.opsForHash().entries(key);
		return map;
	}

	/**
	 * 缓存Map
	 * @param key
	 * @param dataMap
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked", "hiding" })
	public <T> HashOperations<String, Integer, T> setCacheIntegerMap(String key, Map<Integer, T> dataMap) {
		HashOperations hashOperations = redisTemplate.opsForHash();
		if (null != dataMap) {
			for (Map.Entry<Integer, T> entry : dataMap.entrySet()) {
				hashOperations.put(key, entry.getKey(), entry.getValue());
			}
		}
		return hashOperations;
	}

	/**
	 * 获得缓存的Map
	 * @param key
	 * @param hashOperation
	 * @return
	 */
	@SuppressWarnings({ "hiding", "unchecked" })
	public <T> Map<Integer, T> getCacheIntegerMap(String key) {
		Map<Integer, T> map = redisTemplate.opsForHash().entries(key);
		return map;
	}
	
	/**
	 * 删除缓存数据
	 * @param key
	 */
	@SuppressWarnings("unchecked")
	public void delCacheByKey(Object key){
		redisTemplate.delete(key);
	}
	
	/**
	 * 批量删除缓存数据
	 * @param keys
	 */
	@SuppressWarnings("unchecked")
	public void delCacheByCollection(Collections keys){
		redisTemplate.delete(keys);
	}
	
	/**
	 * 判断是否存在
	 * @param key
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public boolean hasKey(Object key){
		return redisTemplate.hasKey(key);
	}
	
	/**
	 * 查询所有缓存key
	 * @param pattern
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Set keys(Object pattern){
		return redisTemplate.keys(pattern);
	}
}
