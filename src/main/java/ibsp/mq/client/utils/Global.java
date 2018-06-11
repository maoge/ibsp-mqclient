package ibsp.mq.client.utils;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibsp.mq.client.api.MQClientImpl;
import ibsp.mq.client.bean.RootUrlBean;
import ibsp.mq.client.event.EventMsg;
import ibsp.mq.client.event.EventSockListener;
import ibsp.mq.client.exception.ZKLockException;
import ibsp.mq.client.router.Router;

public class Global {
	
	private static Logger logger = LoggerFactory.getLogger(Global.class);

	private RootUrlBean urls;
	private SVarObject globalErrObj;
	private int qos;
	private boolean isAuthed;
	private String magicKey;
	
	private volatile boolean isLsnrInited;          // 本机监听是否完成初始化
	private String lsnrIP;                          // 本机和管理平台交互的ip
	private int    lsnrPort;                        // 本机用于接收管理平台下发事件的端口
	private EventSockListener evSockLsnr;           // 管理平台下发事件接收
	
	private AtomicInteger eventCntInQueue;
	private ConcurrentLinkedQueue<EventMsg> eventQueue;
	
	private volatile boolean isTimerRunnerInited;
	private Thread timerEventThread;                // 合并统计TPS、统计上报、EventDisptcher到一个线程处理
	private TimerEventRunner timerEventRunner;
	
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
		
		urls         = new RootUrlBean();
		qos          = SysConfig.get().getMqPrefetchSize();
//		isAuthed     = false;
		//TODO 用户名密码认证还没有做
		isAuthed     = true;
		magicKey     = "";
		globalErrObj = new SVarObject();
		
		routerMap    = new ConcurrentHashMap<String, Router>();
		isLsnrInited = false;
		
		eventCntInQueue = new AtomicInteger(0);
		eventQueue      = new ConcurrentLinkedQueue<EventMsg>();
		
		lsnrPort = CONSTS.LSNR_INVALID_PORT;
	}

	public static Global get() {
		try {
			intanceLock.lock();
			if (theInstance != null) {
				return theInstance;
			} else {
				theInstance = new Global();
				theInstance.initRootUrl();
				theInstance.initListener();
				theInstance.initTimerEventRunner();
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
				if (isTimerRunnerInited) {
					timerEventRunner.stopRunning();
					timerEventThread.join();
					timerEventRunner = null;
					timerEventThread = null;
					isTimerRunnerInited = false;
				}
				
				if (isLsnrInited) {
					if (evSockLsnr != null && evSockLsnr.IsStart()) {
						evSockLsnr.stop();
						evSockLsnr = null;
						isLsnrInited = false;
					}
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			lock.unlock();
		}
	}
	
	public boolean pushEventMsg(EventMsg event) {
		if (event == null)
			return false;
		
		boolean ret = eventQueue.offer(event);
		if (ret) {
			eventCntInQueue.incrementAndGet();
		} else {
			logger.error("event push fail.");
		}
		
		return ret;
	}
	
	public EventMsg popEventMsg() {
		if (eventCntInQueue.get() == 0)
			return null;
		
		EventMsg event = eventQueue.poll();
		if (event != null) {
			eventCntInQueue.decrementAndGet();
		}
		
		return event;
	}
	
	public void registerClient(MQClientImpl client) {
		if (client == null) {
			return;
		}
		
		try {
			lock.lock();
			boolean needCreate = currAllocRouter == null
					|| (currAllocRouter != null && currAllocRouter.getBindSize() >= SysConfig.get().getMqMultiplexingRatio());
			
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
	
	private void initTimerEventRunner() {
		try {
			lock.lock();
			if (isTimerRunnerInited)
				return;
			
			if (timerEventRunner == null)
				timerEventRunner = new TimerEventRunner();
			
			timerEventThread = new Thread(timerEventRunner);
			timerEventThread.setDaemon(true);
			timerEventThread.setName("Router.TimerEventThread");
			timerEventThread.start();
			
			isTimerRunnerInited = true;
		} finally {
			lock.unlock();
		}
	}
	
	private void initListener() {
		if (SysConfig.get().isDebug())
			return;
		
		initLsnrIP();
		if (lsnrIP == null) {
			logger.error("init event listener error:lsnrIP is null!");
			return;
		}
		
		for (int i = 0; i < CONSTS.BATCH_FIND_CNT; i++) {
			try {
				lock.lock();
				if (isLsnrInited)
					break;
				
				initLsnrPort();
				if (lsnrPort == CONSTS.LSNR_INVALID_PORT) {
					logger.error("init event listener error:lsnrPort illigal!");
					continue;
				}
				
				if (evSockLsnr == null) {
					evSockLsnr = new EventSockListener(lsnrIP, lsnrPort);
					evSockLsnr.start();
					isLsnrInited = true;
					
					logger.info("event listener start ok, with addr {}:{}", lsnrIP, lsnrPort);
					break;
				}
			} catch (Exception e) {
				logger.error("Event Socket listener start error:{}, port:{} is used, retry another port.", e.getMessage(), lsnrPort);
				
				try {
					Thread.sleep(100);
				} catch (InterruptedException e1) {
				}
			} finally {
				lock.unlock();
			}
		}
	}
	
	private void initZKLocker() {
		if (SysConfig.get().isMqZKLockerSupport()) {
			zklocker = new ZKLocker();
			zklocker.init();
		}
	}
	
	private void initLsnrIP() {
		SVarObject sVarIP = new SVarObject();

		int cnt = 0;
		boolean ok = false;
		while (cnt < CONSTS.GET_IP_RETRY) {
			int ret = BasicOperation.getLocalIP(sVarIP);
			if (ret == CONSTS.REVOKE_OK) {
				lsnrIP = sVarIP.getVal();
				ok = true;
				break;
			} else {
				try {
					Thread.sleep(CONSTS.GET_IP_RETRY_INTERVAL);
				} catch (InterruptedException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}

		if (!ok) {
			logger.error("BasicOperation.getLocalIP error, localIP use 0.0.0.0");
			lsnrIP = null;
		}
	}
	
	private void initLsnrPort() {
		IVarObject iVarPort = new IVarObject();

		int ret = BasicOperation.getUsablePort(lsnrIP, iVarPort);
		if (ret == CONSTS.REVOKE_OK) {
			lsnrPort = iVarPort.getVal();
		} else {
			logger.error("BasicOperation.getUsablePort error, lsnrPort not initalized.");
			lsnrPort = CONSTS.LSNR_INVALID_PORT;
		}	
	}

	private void initRootUrl() {
		String rootUrls = SysConfig.get().getMqConfRootUrl();
		if (!StringUtils.isNullOrEmtpy(rootUrls)) {
			String[] arr = rootUrls.split(CONSTS.COMMA);

			for (String s : arr) {
				String url = String.format("%s://%s", CONSTS.HTTP_PROTOCAL, s.trim());
				
				if (BasicOperation.checkUrl(url) == CONSTS.REVOKE_OK) {
					urls.putValidUrl(url);
				} else {
					urls.putInvalidUrl(url);
				}
			}
		}
	}
	
	public boolean orderedQueueLock(String nodeName) {
		if (!SysConfig.get().isMqZKLockerSupport()) {
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

	public String getNextUrl() {
		String result = null;
		int retry = 0;
		while (result == null && retry++ < CONSTS.RETRY_CNT) {
			try {
				result = urls.getNextUrl();
				if (result == null) {
					Thread.sleep(CONSTS.RECONNECT_INTERVAL);
				}
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		}
		
		if (result == null) {
			logger.error("Global getNextUrl result null!");
		}
		
		return result;
	}
	
	public void putBrokenUrl(String url) {
		urls.putBrokenUrl(url);
	}
	
	public int getQos() {
		return qos;
	}

	public void setQos(int qos) {
		this.qos = qos;
	}
	
	public void clearAuth() {
		isAuthed = false;
		magicKey = "";
	}
	
	public boolean isAuthed() {
		return isAuthed;
	}

	public void setAuthed(boolean isAuthed) {
		this.isAuthed = isAuthed;
	}
	
	public String getMagicKey() {
		return magicKey;
	}

	public void setMagicKey(String magicKey) {
		this.magicKey = magicKey;
	}

	public void setLastError(String err) {
		globalErrObj.setVal(err);
	}

	public String getLastError() {
		return globalErrObj.getVal();
	}
	
	public String getLsnrIP() {
		return lsnrIP;
	}
	
	public void setLsnrIP(String ip) {
		this.lsnrIP = ip;
	}
	
	public int getLsnrPort() {
		return lsnrPort;
	}
	
	public void setLsnrPort(int port) {
		this.lsnrPort = port;
	}
	
	private class TimerEventRunner implements Runnable {
		
		private volatile boolean bRunning;
		private long lastComputeTS;
		private long lastReportTS;
		private long lastUrlChkTS;
		private long currTS;
		
		public TimerEventRunner() {
			currTS = System.currentTimeMillis();
			lastComputeTS = currTS;
			lastReportTS  = currTS;
			lastUrlChkTS  = currTS;
		}
		
		@Override
		public void run() {
			bRunning = true;
			EventMsg event = null;

			while (bRunning) {
				try {
					if ((event = popEventMsg()) != null) {
						dispachEventMsg(event);
					} else {
						Thread.sleep(CONSTS.EVENT_DISPACH_INTERVAL);
					}
					
					currTS = System.currentTimeMillis();
					
					if ((currTS - lastComputeTS) > CONSTS.STATISTIC_INTERVAL) {
						doCompute();
						lastComputeTS = currTS;
					}
					
					if ((currTS - lastReportTS) > CONSTS.REPORT_INTERVAL) {
						doReport();
						lastReportTS = currTS;
					}
					
					if ((currTS - lastUrlChkTS) > CONSTS.RECONNECT_INTERVAL) {
						doUrlCheck();
						lastUrlChkTS = currTS;
					}
					
//					for (Router router : routerMap.values()) {
//						if (currTS - router.getLastWriteTime() > SysConfig.get().getMqWriteTimeout()) {
//							try {
//								VBroker vbroker = router.getCurrentVBroker();
//								if (vbroker.forceClose()==CONSTS.REVOKE_OK) {
//									logger.warn("RabbitMQ client write timeout, close send channel by force, VBrokerID: ", vbroker.getVBrokerId());
//									router.setLastWriteTime(Long.MAX_VALUE);
//								} else {
//									logger.warn("RabbitMQ client write timeout, failed to close send channel...");
//								}
//							} catch (Exception e) {
//								logger.warn("RabbitMQ client write timeout, failed to close send channel...", e);
//							}
//						}
//					}
				} catch(Exception e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		
		public void stopRunning() {
			bRunning = false;
		}
		
		private void dispachEventMsg(EventMsg event) {
			Set<Entry<String, Router>> entrySet = routerMap.entrySet();
			for (Entry<String, Router> entry : entrySet) {
				Router router = entry.getValue();
				router.dispachEventMsg(event);
			}
		}
		
		private void doCompute() {
			Set<Entry<String, Router>> entrySet = routerMap.entrySet();
			for (Entry<String, Router> entry : entrySet) {
				Router router = entry.getValue();
				router.doCompute();
			}
		}
		
		private void doReport() {
			Set<Entry<String, Router>> entrySet = routerMap.entrySet();
			for (Entry<String, Router> entry : entrySet) {
				Router router = entry.getValue();
				router.doReport();
			}
		}
		
		private void doUrlCheck() {
			urls.doUrlCheck();
		}
		
	}

}
