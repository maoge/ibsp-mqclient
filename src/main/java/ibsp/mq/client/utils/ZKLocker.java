package ibsp.mq.client.utils;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibsp.common.utils.CONSTS;
import ibsp.common.utils.IBSPConfig;
import ibsp.mq.client.exception.ZKLockException;

public class ZKLocker {
	
	private static Logger logger = LoggerFactory.getLogger(ZKLocker.class);
	
	private CuratorFramework curator = null;
	private Map<String, ZKConnStatListener> lsnrMap = null;
	private ReentrantLock lock = null;
	
	public ZKLocker() {
		lock = new ReentrantLock();
		lsnrMap = new ConcurrentHashMap<String, ZKConnStatListener>();
	}
	
	public void init() {
		curator = CuratorFrameworkFactory.newClient(IBSPConfig.getInstance().getMqZKRootUrl(),
				CONSTS.ZK_SESSION_TIMEOUT, CONSTS.ZK_CONN_TIMEOUT,
				new RetryNTimes(CONSTS.ZK_CONN_RETRY, CONSTS.ZK_CONN_RETRY_INTERVAL));
		curator.start();
	}
	
	public boolean lock(String nodeName) throws ZKLockException {
		String path = String.format("%s/%s", CONSTS.ZK_LOCK_ROOTPATH, nodeName);
		boolean ret = false;
		
		try {
			lock.lock();
			
			if (curator == null) {
				init();
			}
			
			if (lsnrMap.get(path) != null) {
				logger.info("{} already locked.", path);
				return false;
			}
			
			Stat stat = curator.checkExists().forPath(path);
			if (stat == null) {
				curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
				
				ZKConnStatListener listener = new ZKConnStatListener(path);
				curator.getConnectionStateListenable().addListener(listener);
				lsnrMap.put(path, listener);
				
				ret = true;
				
				logger.info("lock path:{} success!", path);
			} else {
				String err = String.format("node:%s have locked", path);
				Global.get().setLastError(err);
			}
		} catch (Exception e) {
			String err = String.format("node:%s lock error, {}", path, e.getMessage());
			throw new ZKLockException(err, new Exception());
		} finally {
			lock.unlock();
		}
		
		return ret;
	}
	
	public boolean unlock(String nodeName) throws ZKLockException {
		String path = String.format("%s/%s", CONSTS.ZK_LOCK_ROOTPATH, nodeName);
		boolean ret = false;
		
		try {
			lock.lock();
			
			ZKConnStatListener listener = lsnrMap.get(path);
			if (listener == null) {
				logger.info("node:{} not locked, no need to unlock!", path);
				return true;
			}
			
			curator.getConnectionStateListenable().removeListener(listener);
			lsnrMap.remove(path);
			
			Stat stat = curator.checkExists().forPath(path);
			if (stat != null) {
				curator.delete().forPath(path);
				ret = true;
				
				logger.info("node:{} unlocked success!", path);
			}
		} catch (Exception e) {
			String err = String.format("node:%s unlock error, {}", path, e.getMessage());
			throw new ZKLockException(err, new Exception());
		} finally {
			lock.unlock();
		}
		
		return ret;
	}
	
	public boolean isLocked(String nodeName) {
		boolean ret = false;
		
		try {
			lock.lock();
			
			String path = String.format("%s/%s", CONSTS.ZK_LOCK_ROOTPATH, nodeName);
			ZKConnStatListener listener = lsnrMap.get(path);
			if (listener != null) {
				ret = true;
			}
			
		} finally {
			lock.unlock();
		}
		
		return ret;
	}
	
	public void destroy() {
		try {
			lock.lock();
			
			if (curator != null) {
				Set<Entry<String, ZKConnStatListener>> lsnrEnterySet = lsnrMap.entrySet();
				for (Entry<String, ZKConnStatListener> entry : lsnrEnterySet) {
					ZKConnStatListener lsnr = entry.getValue();
					String path = entry.getKey();
					
					if (lsnr != null) {
						curator.getConnectionStateListenable().removeListener(lsnr);
					}
					
					Stat stat = curator.checkExists().forPath(path);
					if (stat != null) {
						curator.delete().forPath(path);
					}
				}
				
				CloseableUtils.closeQuietly(curator);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			lock.unlock();
		}
	}
	
}
