package com.ffcs.mq.client.bean;

import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ffcs.mq.client.utils.BasicOperation;
import com.ffcs.mq.client.utils.CONSTS;

public class RootUrlBean {
	
	private static Logger logger = LoggerFactory.getLogger(RootUrlBean.class);

	private Vector<String> valildUrlVec;
	private Vector<String> invalildUrlVec;

	private volatile int validSize;
	private volatile int invalidSize;
	private long lastIndex;
	
	private ReentrantLock lock;

	public RootUrlBean() {
		valildUrlVec   = new Vector<String>();
		invalildUrlVec = new Vector<String>();
		validSize      = 0;
		invalidSize    = 0;
		lastIndex      = 0L;
		lock           = new ReentrantLock();
	}
	
	public void putValidUrl(String url) {
		try {
			lock.lock();
			valildUrlVec.add(url);
			validSize++;
		} finally {
			lock.unlock();
		}
	}
	
	public void putInvalidUrl(String url) {
		try {
			lock.lock();
			invalildUrlVec.add(url);
			invalidSize++;
		} finally {
			lock.unlock();
		}
	}
	
	public void mergeRecovered(int idx) {
		try {
			lock.lock();
			if (idx < 0 || idx > invalidSize - 1)
				return;
			
			String url = invalildUrlVec.remove(idx);
			invalidSize--;
			
			valildUrlVec.add(url);
			validSize++;
		} finally {
			lock.unlock();
		}
	}
	
	public void putBrokenUrl(String url) {
		try {
			lock.lock();
			int idx = valildUrlVec.indexOf(url);
			
			if (idx < 0 || idx > validSize - 1)
				return;
			
			valildUrlVec.remove(idx);
			validSize--;
			
			invalildUrlVec.add(url);
			invalidSize++;
		} finally {
			lock.unlock();
		}
	}

	public String getNextUrl() {
		if (validSize == 0) {
			logger.error("no valid root url!");
			return null;
		}

		try {
			lock.lock();
			int idx = (validSize == 1) ? 0 : (int) (lastIndex++ % validSize);
			return valildUrlVec.get(idx);
		}  finally {
			lock.unlock();
		}
	}
	
	public void doUrlCheck() {
		if (invalidSize <= 0) {
			return;
		}
		
		try {
			int idx = invalidSize - 1;
			for (; idx >= 0; idx--) {
				String url = invalildUrlVec.get(idx);
				if (BasicOperation.checkUrl(url) == CONSTS.REVOKE_OK) {
					mergeRecovered(idx);
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

}
