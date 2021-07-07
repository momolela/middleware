package com.momolela.distributedlock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public abstract class AbstractLock implements Lock{
	
	protected volatile boolean locked;
	
	private Thread exclusiveOwnerThread;

	@Override
	public void lock() {
		try {
			lock(false, 0, null, false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		lock(false, 0, null, true);
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		try {
			return lock(true, time, unit, false);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public void unlock() {
		// 检查当前线程是否持有锁
		if (Thread.currentThread() != getExclusiveOwnerThread()) {
			throw new IllegalMonitorStateException("current thread does not hold the lock");
		}
		unlock0();
		setExclusiveOwnerThread(null);
	}

	protected void setExclusiveOwnerThread(Thread thread) {
		exclusiveOwnerThread = thread;
	}
	
	protected final Thread getExclusiveOwnerThread() {
		return exclusiveOwnerThread;
	}
	
	protected abstract void unlock0();
	
	protected abstract boolean lock(boolean useTimeout, long time, TimeUnit unit, boolean interrupt) throws InterruptedException;

}
