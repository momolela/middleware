package com.momolela.pipeline.redisson;


import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.BatchResult;
import org.redisson.api.RBatch;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class usePipelineInRedisson {
	@Test
	public void testPipeline(){
		Config config = new Config();
		config. useSingleServer().setAddress("redis://127.0.0.1:6379").setPassword("bship");
		RedissonClient redisson = Redisson.create(config);
		
		RBatch batch = redisson.createBatch();
		batch.getSet("setkey").readAllAsync();
		batch.getList("listkey").readAllAsync();
		batch.getMap("hashkey").addAndGetAsync("hashfiled", 2);
		batch.getAtomicDouble("key1");
		batch.getAtomicLong("key2");
		batch.getBitSet("key3");
		
		BatchResult<?> res = batch.execute();
		
		for(int i = 0;i<res.size();i++){
			System.out.println(res.get(i));
		}
	}
}
