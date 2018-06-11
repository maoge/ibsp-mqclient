package ibsp.mq.client.router;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibsp.mq.client.bean.QueueDtlBean;
import ibsp.mq.client.utils.CONSTS;

public class VBrokerReconnector {
	
	private static Logger logger = LoggerFactory.getLogger(VBrokerReconnector.class);

	private AtomicBoolean bRunning;
	private VBrokerGroup vbGroup;
	private List<String> brokenVBList; // 待修复 vbrokerId列表(单机的)
	private List<String> multiNodeVBList;  // 待修复 vbrokerId列表(镜像集群的)
	private volatile int cnt;

	private ScheduledExecutorService executor;
	private ReconnectRunner runner;

	private ReentrantLock lock;

	public VBrokerReconnector(VBrokerGroup vbGroup) {
		this.vbGroup = vbGroup;
		brokenVBList = new ArrayList<String>();
		multiNodeVBList = new ArrayList<String>();
		lock = new ReentrantLock();
		cnt = 0;

		executor = Executors.newSingleThreadScheduledExecutor();
		runner = new ReconnectRunner();
		executor.scheduleAtFixedRate(runner, CONSTS.RECONNECT_INTERVAL, CONSTS.RECONNECT_INTERVAL, TimeUnit.MILLISECONDS);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				if (executor != null) {
					executor.shutdown();
				}
			}
		});

		bRunning = new AtomicBoolean(false);
	}

	public boolean IsRunning() {
		return bRunning.get();
	}

	public boolean ShutdownExecutor() {
		boolean ret = false;

		if (executor != null && !executor.isShutdown()) {
			executor.shutdown();
			executor = null;
		}

		return ret;
	}

	public int GetBrokerCount() {
		return cnt;
	}

	public boolean AddBrokenVBroker(String vbrokerId, boolean immediate) {
		boolean ret = false;
		
		try {
			lock.lock();
			if (immediate) {
				ret = brokenVBList.add(vbrokerId);
				if (ret) {
					cnt++;
				}
			} else {
				// 如果是集群则先放入multiNodeVBList, 待ha switch事件更新最新的VBroker的新主节点ID后在放入brokenVBList
				ret = multiNodeVBList.add(vbrokerId);
			}
		} finally {
			lock.unlock();
		}

		return ret;
	}
	
	// when shrink vbroker and vbroker is down, must reomve from Reconnector
	public boolean removeBrokenVBroker(String vbrokerId) {
		boolean ret = false;
		
		try {
			lock.lock();
			if (brokenVBList.contains(vbrokerId)) {
				brokenVBList.remove(vbrokerId);
				cnt--;
				ret = true;
			} else if (multiNodeVBList.contains(vbrokerId)) {
				multiNodeVBList.remove(vbrokerId);
				ret = true;
			}
		} finally {
			lock.unlock();
		}
		
		return ret;
	}
	
	public boolean MoveMultiNodeVBroker(String vbrokerId) {
		boolean ret = false;
		
		try {
			lock.lock();
			int idx = multiNodeVBList.indexOf(vbrokerId);
			if (idx != -1) {
				multiNodeVBList.remove(idx);
			}
			
			if (brokenVBList.indexOf(vbrokerId) == -1) {  // 不存在则处理, 已经存在了则pass
				brokenVBList.add(vbrokerId);
				cnt++;
			}
			
			ret = true;
		} finally {
			lock.unlock();
		}
		
		return ret;
	}

	private class ReconnectRunner implements Runnable {

		private Vector<String> successVec;

		public ReconnectRunner() {
			super();

			successVec = new Vector<String>();
		}

		@Override
		public void run() {
			if (cnt == 0) {
				vbGroup.shutdownReconncetor();
				return;
			}

			for (int idx = cnt - 1; idx >= 0; idx--) {
				String vbrokerId = brokenVBList.get(idx);
				VBroker vbroker = vbGroup.getVBrokerById(vbrokerId);
				if (vbroker == null)
					continue;

				try {
					if (vbroker.connect() == CONSTS.REVOKE_OK) {
						HashMap<String, QueueDtlBean> queueDtlMap = new HashMap<String, QueueDtlBean>();
						vbGroup.cloneQueueDtlMap(queueDtlMap, vbrokerId);
						
						if (vbroker.relistenAfterBroken(queueDtlMap) == CONSTS.REVOKE_OK) {
							vbGroup.addToRepairedNodes(vbrokerId);
							successVec.add(vbrokerId);

							logger.info("vbroker id:{} reconnect.", vbrokerId);
						} else {
							vbroker.softcut();
						}
					} else {
						continue;
					}
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
			}

			for (String s : successVec) {
				try {
					lock.lock();
					boolean ret = brokenVBList.remove(s);
					if (ret) {
						cnt--;
					}

				} finally {
					lock.unlock();
				}
			}

			successVec.clear();
		}

	}

}
