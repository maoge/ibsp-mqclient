package ibsp.mq.client.utils;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibsp.common.events.EventController;
import ibsp.common.utils.CONSTS;
import ibsp.common.utils.IBSPConfig;
import ibsp.common.utils.SVarObject;
import ibsp.mq.client.api.MQClientImpl;
import ibsp.mq.client.event.EventMsg;
import ibsp.mq.client.exception.ZKLockException;
import ibsp.mq.client.router.Router;

public class Global {
	
	private static Logger logger = LoggerFactory.getLogger(Global.class);

	private SVarObject globalErrObj;
	private int qos;
	
	private Router currAllocRouter = null;          // 当前待复用的router
	private Map<String, Router> routerMap = null;   // 所有已分配的Router
	
	private ZKLocker zklocker = null;
	private ReentrantLock lock = null;
	
	private static Global theInstance = null;
	private static ReentrantLock intanceLock = null;

	static {
		intanceLock = new ReentrantLock();
	}

	public Global() {
		lock = new ReentrantLock();
		
		qos          = IBSPConfig.getInstance().getMqPrefetchSize();
		globalErrObj = new SVarObject();
		routerMap    = new ConcurrentHashMap<String, Router>();
	}

	public static Global get() {
		try {
			intanceLock.lock();
			if (theInstance != null) {
				return theInstance;
			} else {
				theInstance = new Global();
				theInstance.initZKLocker();
			}
		} finally {
			intanceLock.unlock();
		}

		return theInstance;
	}
	
	public void shutdown() {
		try {
			lock.lock();
			if (routerMap.size() == 0) {
				EventController.getInstance().unsubscribe(CONSTS.TYPE_MQ_CLIENT);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			lock.unlock();
		}
	}
	
	public void registerClient(MQClientImpl client) {
		if (client == null) {
			return;
		}
		
		try {
			lock.lock();
			boolean needCreate = currAllocRouter == null
					|| (currAllocRouter != null && currAllocRouter.getBindSize() >= IBSPConfig.getInstance().getMqMultiplexingRatio());
			
			if (needCreate) {
				currAllocRouter = new Router();
				routerMap.put(currAllocRouter.getRouterID(), currAllocRouter);
			}
			
			currAllocRouter.addBindClient(client.getClientID());
			client.setRouter(currAllocRouter);
		} finally {
			lock.unlock();
		}
	}
	
	public Router getAllocRouter() {
		return currAllocRouter;
	}
	
	public void removeRouter(String routerID) {
		try {
			lock.lock();
			routerMap.remove(routerID);
		} finally {
			lock.unlock();
		}
	}
	
	private void initZKLocker() {
		if (IBSPConfig.getInstance().MqIsMqZKLockerSupport()) {
			zklocker = new ZKLocker();
			zklocker.init();
		}
	}
	
	public boolean orderedQueueLock(String nodeName) {
		if (!IBSPConfig.getInstance().MqIsMqZKLockerSupport()) {
			String err = String.format("mq.zklocker.support is not true!", nodeName);
			setLastError(err);
			logger.error(err);
			
			return false;
		}
		
		boolean locked = false;
		if (zklocker.isLocked(nodeName)) {
			String err = String.format("node:%s haved locked!", nodeName);
			setLastError(err);
			logger.error(err);
			
			return false;
		}
		
		try {
			locked = zklocker.lock(nodeName);
		} catch (ZKLockException e) {
			String err = e.getMessage();
			setLastError(err);
			logger.error(err);
		}
		
		return locked;
	}
	
	public boolean orderedQueueUnlock(String nodeName) {
		boolean unlocked = false;
		if (!zklocker.isLocked(nodeName)) {
			String err = String.format("node:%s not locked, not need to unlock!", nodeName);
			logger.error(err);
			
			return true;
		}
		
		try {
			unlocked = zklocker.unlock(nodeName);
		} catch (ZKLockException e) {
			String err = e.getMessage();
			setLastError(err);
			logger.error(err);
		}
		
		return unlocked;
	}

	public int getQos() {
		return qos;
	}

	public void setQos(int qos) {
		this.qos = qos;
	}

	public void setLastError(String err) {
		globalErrObj.setVal(err);
	}

	public String getLastError() {
		return globalErrObj.getVal();
	}

	public Map<String, Router> getRouterMap() {
		return routerMap;
	}

	public void dispachEventMsg(EventMsg event) {
		Set<Entry<String, Router>> entrySet = routerMap.entrySet();
		for (Entry<String, Router> entry : entrySet) {
			Router router = entry.getValue();
			router.dispachEventMsg(event);
		}
	}

}
