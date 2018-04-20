package com.ffcs.mq.client.router;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ffcs.mq.client.api.MQMessage;
import com.ffcs.mq.client.bean.QueueDtlBean;
import com.ffcs.mq.client.utils.BasicOperation;
import com.ffcs.mq.client.utils.CONSTS;
import com.ffcs.mq.client.utils.Global;
import com.ffcs.mq.client.utils.LVarObject;
import com.ffcs.mq.client.utils.SVarObject;
import com.ffcs.mq.client.utils.StringUtils;

public class VBrokerGroup {

	private static Logger logger = LoggerFactory.getLogger(VBrokerGroup.class);

	private Map<String, VBroker> vbrokerMap; // vbrokerId -> vbroker

	private String groupId;
	private String groupName;

	private Vector<String> validNodes;       // 可用的vbrokerId列表
	private Vector<String> invalidNodes;     // 不可用的vbrokerId列表
	private Vector<String> repairedNodes;    // 已修复的vbrokerId列表

	private AtomicInteger validSize;         // validNodes
	private AtomicInteger invalidSize;       // invalidNodes.size()
	private AtomicInteger repairedSize;      // repairedNodes.size()

	private Map<String, LVarObject> rbSendSeedMap; // Round-Robin 方式均衡发送
	private volatile long rbRevSeed;         // Round-Robin 方式均衡接收

	private VBrokerReconnector reconnector;  // 重连

	private StringBuilder rptStrBuilder;

	private ReentrantLock lock;
	
	private boolean allConnected = false;

	private String currentVBrokerID = null;
	
	public VBrokerGroup(String groupId, String groupName) {
		this.groupId = groupId;
		this.groupName = groupName;

		vbrokerMap = new HashMap<String, VBroker>();

		validNodes = new Vector<String>();
		invalidNodes = new Vector<String>();
		repairedNodes = new Vector<String>();

		validSize = new AtomicInteger(0);
		invalidSize = new AtomicInteger(0);
		repairedSize = new AtomicInteger(0);
		
		rbRevSeed = 0L;
		rbSendSeedMap = new HashMap<String, LVarObject>();

		lock = new ReentrantLock();
		rptStrBuilder = new StringBuilder();
	}
	
	public void startReconncetor() {
		if (reconnector == null) {
			reconnector = new VBrokerReconnector(this);
		}
	}
	
	public void shutdownReconncetor() {
		try {
			lock.lock();
			if (reconnector != null) {
				reconnector.ShutdownExecutor();
				reconnector = null;
			}
		} finally {
			lock.unlock();
		}
	}

	public VBroker getVBrokerById(String vbrokerId) {
		return vbrokerMap.get(vbrokerId);
	}
	
	public VBroker getCurrentVBroker() {
		return getVBrokerById(this.currentVBrokerID);
	}

	public void addToInvalidNodes(VBroker vbroker) {
		if (vbroker == null)
			return;

		String vbrokerId = vbroker.getVBrokerId();
		if (vbrokerMap.containsKey(vbrokerId))
			return;
		
		try {
			lock.lock();
			vbrokerMap.put(vbrokerId, vbroker);
			
			invalidNodes.add(vbrokerId);
			invalidSize.incrementAndGet();
		} finally {
			lock.unlock();
		}
	}

	public void addToRepairedNodes(String vbrokerId) {
		try {
			lock.lock();
			invalidNodes.remove(vbrokerId);
			repairedNodes.add(vbrokerId);

			invalidSize.decrementAndGet();
			repairedSize.incrementAndGet();
		} finally {
			lock.unlock();
		}
	}
	
	public void addToRepairedDirectly(String vbrokerId) {
		try {
			lock.lock();
			invalidNodes.remove(vbrokerId);
			repairedNodes.add(vbrokerId);

			invalidSize.decrementAndGet();
			repairedSize.incrementAndGet();
		} finally {
			lock.unlock();
		}
	}

	public boolean connect() {
		if (allConnected)
			return true;
		
		int size = invalidSize.get();
		int idx = size - 1;
		int succ = 0;
		for (int i = idx; i >= 0; i--) {
			String vbrokerId = invalidNodes.get(i);
			VBroker vbroker = vbrokerMap.get(vbrokerId);
			if (vbroker == null) {
				logger.error("vbrokerId:{} can not get VBroker instance.", vbrokerId);
				invalidNodes.remove(i);
				continue;
			}

			boolean ok = false;
			int cnt = 0;
			while (!ok && cnt++ < CONSTS.RETRY_CNT) {
				ok = vbroker.connect() == CONSTS.REVOKE_OK;
				if (!ok) {
					try {
						Thread.sleep(CONSTS.RETRY_INTERVAL);
					} catch (InterruptedException e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
			
			if (!ok) {
				logger.error("connect vbroker id:{} fail.", vbroker.getVBrokerId());
				processConnErr(vbroker);
				continue;
			}

			try {
				lock.lock();
				succ++;
				
				validNodes.add(vbrokerId);
				invalidNodes.remove(i);

				invalidSize.decrementAndGet();
				validSize.incrementAndGet();
			} finally {
				lock.unlock();
			}
		}
		
		if (invalidSize.get() == 0)
			allConnected = true;
		
		return succ > 0;
	}

	public boolean close() {
		try {
			lock.lock();
			for (String vbrokerId : validNodes) {
				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (vbroker == null) {
					logger.error("vbroker data error: VBroker is null, vbrokerId:{}.", vbrokerId);
					continue;
				}

				vbroker.close();
			}

			for (String vbrokerId : repairedNodes) {
				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (vbroker == null) {
					logger.error("vbroker data error: VBroker is null, vbrokerId:{}.", vbrokerId);
					continue;
				}

				vbroker.close();
			}

			if (reconnector != null) {
				reconnector.ShutdownExecutor();
				reconnector = null;
			}
			
			allConnected = false;
		} finally {
			lock.unlock();
		}

		return false;
	}

	public int listenQueue(String queueName) {
		// move repairdNodes to validNodes
		doMerge();

		int successCnt = 0;

		try {
			lock.lock();
			
			int idx = 0;
			for (String vbrokerId : validNodes) {
				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (vbroker == null) {
					logger.error("vbroker data error: VBroker is null, vbrokerId:{}.", vbrokerId);
					idx++;
					continue;
				}

				if (vbroker.listenQueue(queueName) == CONSTS.REVOKE_OK) {
					successCnt++;
				} else {
					Broker broker = vbroker.getMasterBroker();
					String err = String.format("ListenQueue:%s failed, broker_id:%s ip:%s port:%d.",
							queueName, broker.getBrokerId(), broker.getIP(), broker.getPort());
					logger.error(err);
					
					processErrVBroker(vbroker, idx, true);
				}
				
				idx++;
			}
		} finally {
			lock.unlock();
		}

		return successCnt > 0 ? CONSTS.REVOKE_OK : CONSTS.REVOKE_NOK;
	}

	public int unlistenQueue(String queueName) {
		int res = CONSTS.REVOKE_NOK;

		// move repairdNodes to validNodes
		doMerge();

		boolean allOk = true;

		try {
			lock.lock();
			for (String vbrokerId : validNodes) {
				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (vbroker == null) {
					logger.error("vbroker data error: VBroker is null, vbrokerId:{}.", vbrokerId);
					continue;
				}

				if (vbroker.unlistenQueue(queueName) == CONSTS.REVOKE_NOK) {
					allOk = false;
				}
			}
		} finally {
			lock.unlock();
		}

		if (allOk) {
			res = CONSTS.REVOKE_OK;
		}

		return res;
	}

	public int listenTopicAnonymous(String topic) {
		// move repairdNodes to validNodes
		doMerge();

		int successCnt = 0;

		try {
			lock.lock();
			
			int idx = 0;
			for (String vbrokerId : validNodes) {
				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (vbroker == null) {
					logger.error("vbroker data error: VBroker is null, vbrokerId:{}.", vbrokerId);
					idx++;
					continue;
				}

				if (vbroker.listenTopicAnonymous(topic) == CONSTS.REVOKE_OK) {
					successCnt++;
				} else {
					Broker broker = vbroker.getMasterBroker();
					String err = String.format("listenTopicAnonymous:%s failed, broker_id:%s ip:%s port:%d.",
							topic, broker.getBrokerId(), broker.getIP(), broker.getPort());
					logger.error(err);
					
					processErrVBroker(vbroker, idx, true);
				}
				
				idx++;
			}
		} finally {
			lock.unlock();
		}

		return successCnt > 0 ? CONSTS.REVOKE_OK : CONSTS.REVOKE_NOK;
	}

	public int unlistenTopicAnonymous(String topic) {
		int res = CONSTS.REVOKE_NOK;

		// move repairdNodes to validNodes
		doMerge();

		boolean allOk = true;

		try {
			lock.lock();
			for (String vbrokerId : validNodes) {
				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (vbroker == null) {
					logger.error("vbroker data error: VBroker is null, vbrokerId:{}.", vbrokerId);
					continue;
				}

				if (vbroker.unlistenTopicAnonymous(topic) == CONSTS.REVOKE_NOK) {
					allOk = false;
				}
			}
		} finally {
			lock.unlock();
		}

		if (allOk) {
			res = CONSTS.REVOKE_OK;
		}

		return res;
	}

	private int createAndBind(String topic, String realQueueName) {
		int res = CONSTS.REVOKE_NOK;
		
		// move repairdNodes to validNodes
		doMerge();

		
		try {
			lock.lock();
			boolean allOk = true;
			Vector<VBroker> succVec = new Vector<VBroker>();

			for (String vbrokerId : validNodes) {
				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (vbroker == null) {
					logger.error("vbroker data error: VBroker is null, vbrokerId:{}.", vbrokerId);
					continue;
				}

				if (vbroker.createAndBind(topic, realQueueName) != CONSTS.REVOKE_OK) {
					allOk = false;
					break;
				} else {
					succVec.add(vbroker);
				}
			}

			if (allOk) {
				res = CONSTS.REVOKE_OK;
			} else {
				for (VBroker vbroker : succVec) {
					vbroker.unBindAndDelete(topic, realQueueName);
				}
			}

			succVec.clear();
		} finally {
			lock.unlock();
		}

		return res;
	}

	public int unBindAndDelete(String consumerId, String topic, String realQueueName) {
		int res = CONSTS.REVOKE_NOK;

		// move repairdNodes to validNodes
		doMerge();

		if (invalidSize.get() > 0 || repairedSize.get() > 0) {
			Global.get().setLastError(CONSTS.ERR_NOT_ALL_NODES_RDY);
			return CONSTS.REVOKE_NOK;
		}

		boolean allOk = true;
		try {
			lock.lock();
			for (String vbrokerId : validNodes) {
				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (vbroker == null) {
					String err = String.format("vbrokerId:%d data null.", vbrokerId);
					logger.error(err);
					continue;
				}

				if (vbroker.unBindAndDelete(topic, realQueueName) != CONSTS.REVOKE_OK) {
					allOk = false;
				}
			}

			if (allOk) {
				SVarObject sVarDel = new SVarObject();
				if (BasicOperation.delPermnentTopic(consumerId, sVarDel) == CONSTS.REVOKE_OK) {
					res = CONSTS.REVOKE_OK;
				} else {
					String err = sVarDel.getVal();
					logger.error(err);
					Global.get().setLastError(err);
				}
			}

		} finally {
			lock.unlock();
		}

		return res;
	}

	public int listenTopicPermnent(String topic, String consumerId) {
		int res = CONSTS.REVOKE_NOK;

		// move repairdNodes to validNodes
		doMerge();

		if (invalidSize.get() > 0 || repairedSize.get() > 0) {
			Global.get().setLastError(CONSTS.ERR_NOT_ALL_NODES_RDY);
			return CONSTS.REVOKE_NOK;
		}

		boolean allOk = true;
		Set<String> listenSuccId = new HashSet<String>();

		String realQueueName = null;
		boolean needCreate = false;
		SVarObject sVar0 = new SVarObject(); // SRC_QUEUE
		SVarObject sVar1 = new SVarObject(); // REAL_QUEUE
		SVarObject sVar2 = new SVarObject(); // MAIN_KEY
		SVarObject sVar3 = new SVarObject(); // SUB_KEY
		SVarObject sVar4 = new SVarObject(); // GROUP_ID
		if (BasicOperation.getPermnentTopic(consumerId, sVar0, sVar1, sVar2, sVar3, sVar4) == CONSTS.REVOKE_OK) {
			realQueueName = sVar1.getVal();
		} else {
			SVarObject sVarRealQueue = new SVarObject();
			if (BasicOperation.genPermQueue(sVarRealQueue) == CONSTS.REVOKE_OK) {
				realQueueName = sVarRealQueue.getVal();
				needCreate = true;
			} else {
				logger.error("call genPermQueue error.");
				return CONSTS.REVOKE_NOK;
			}
		}

		if (needCreate) {
			if (createAndBind(topic, realQueueName) == CONSTS.REVOKE_OK) {
				SVarObject sVarPut = new SVarObject();
				String mainKey = topic;
				String subKey = topic;
				if (BasicOperation.putPermnentTopic(consumerId, topic, realQueueName, mainKey, subKey, this.groupId, sVarPut) == CONSTS.REVOKE_NOK) {
					Global.get().setLastError(sVarPut.getVal());

					SVarObject sVarDel = new SVarObject();
					BasicOperation.delPermnentTopic(consumerId, sVarDel);
					return CONSTS.REVOKE_NOK;
				}
			} else {
				return CONSTS.REVOKE_NOK;
			}
		}

		try {
			lock.lock();
			for (String vbrokerId : validNodes) {
				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (vbroker == null) {
					logger.error("vbroker data error: VBroker is null, vbrokerId:{}.", vbrokerId);
					continue;
				}

				if (vbroker.listenTopicPermnent(topic, realQueueName, consumerId) == CONSTS.REVOKE_OK) {
					listenSuccId.add(vbroker.getVBrokerId());
				} else {
					allOk = false;
					break;
				}
			}
		} finally {
			if (!allOk) {
				for (String vbrokerId : listenSuccId) {
					VBroker vbroker = vbrokerMap.get(vbrokerId);
					if (vbroker != null) {
						vbroker.unlistenTopicPermnent(topic, consumerId);
					}
				}
			}

			listenSuccId.clear();
			lock.unlock();
		}

		if (allOk) {
			res = CONSTS.REVOKE_OK;
		}

		return res;
	}

	public int unlistenTopicPermnent(String topic, String consumerId) {
		int res = CONSTS.REVOKE_NOK;

		// move repairdNodes to validNodes
		doMerge();

		boolean allOk = true;

		try {
			lock.lock();
			for (String vbrokerId : validNodes) {
				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (vbroker == null) {
					logger.error("vbroker data error: VBroker is null, vbrokerId:{}.", vbrokerId);
					continue;
				}

				if (vbroker.unlistenTopicPermnent(topic, consumerId) == CONSTS.REVOKE_NOK) {
					allOk = false;
				}
			}
		} finally {
			lock.unlock();
		}

		if (allOk) {
			res = CONSTS.REVOKE_OK;
		}

		return res;
	}

	public int listenTopicWildcard(String mainKey, String subKey, String consumerId) {
		int res = CONSTS.REVOKE_NOK;

		// move repairdNodes to validNodes
		doMerge();

		if (invalidSize.get() > 0 || repairedSize.get() > 0) {
			Global.get().setLastError(CONSTS.ERR_NOT_ALL_NODES_RDY);
			return CONSTS.REVOKE_NOK;
		}

		boolean allOk = true;
		Set<String> listenSuccId = new HashSet<String>();

		String realQueueName = null;
		boolean needCreate = false;
		SVarObject sVar0 = new SVarObject(); // SRC_QUEUE
		SVarObject sVar1 = new SVarObject(); // REAL_QUEUE
		SVarObject sVar2 = new SVarObject(); // MAIN_KEY
		SVarObject sVar3 = new SVarObject(); // SUB_KEY
		SVarObject sVar4 = new SVarObject(); // GROUP_ID
		if (BasicOperation.getPermnentTopic(consumerId, sVar0, sVar1, sVar2, sVar3, sVar4) == CONSTS.REVOKE_OK) {
			realQueueName = sVar1.getVal();
		} else {
			SVarObject sVarRealQueue = new SVarObject();
			if (BasicOperation.genPermQueue(sVarRealQueue) == CONSTS.REVOKE_OK) {
				realQueueName = sVarRealQueue.getVal();
				needCreate = true;
			} else {
				logger.error("call genPermQueue error.");
				return CONSTS.REVOKE_NOK;
			}
		}

		if (needCreate) {
			if (createAndBind(subKey, realQueueName) == CONSTS.REVOKE_OK) {
				SVarObject sVarPut = new SVarObject();
				if (BasicOperation.putPermnentTopic(consumerId, mainKey, realQueueName, mainKey, subKey, this.groupId, sVarPut) == CONSTS.REVOKE_NOK) {
					Global.get().setLastError(sVarPut.getVal());

					SVarObject sVarDel = new SVarObject();
					BasicOperation.delPermnentTopic(consumerId, sVarDel);
					return CONSTS.REVOKE_NOK;
				}
			} else {
				return CONSTS.REVOKE_NOK;
			}
		}

		try {
			lock.lock();
			for (String vbrokerId : validNodes) {
				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (vbroker == null) {
					logger.error("vbroker data error: VBroker is null, vbrokerId:{}.", vbrokerId);
					continue;
				}

				// 到此与listenTopicPermnent同
				if (vbroker.listenTopicPermnent(subKey, realQueueName, consumerId) == CONSTS.REVOKE_OK) {
					listenSuccId.add(vbroker.getVBrokerId());
				} else {
					allOk = false;
					break;
				}
			}
		} finally {
			if (!allOk) {
				for (String vbrokerId : listenSuccId) {
					VBroker vbroker = vbrokerMap.get(vbrokerId);
					if (vbroker != null) {
						vbroker.unlistenTopicPermnent(subKey, consumerId);
					}
				}
			}

			listenSuccId.clear();
			lock.unlock();
		}

		if (allOk) {
			res = CONSTS.REVOKE_OK;
		}

		return res;
	}

	public int unlistenTopicWildcard(String topic, String consumerId) {
		int res = CONSTS.REVOKE_NOK;

		// move repairdNodes to validNodes
		doMerge();

		boolean allOk = true;

		try {
			lock.lock();
			for (String vbrokerId : validNodes) {
				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (vbroker == null) {
					logger.error("vbroker data error: VBroker is null, vbrokerId:{}.", vbrokerId);
					continue;
				}

				// 到此与listenTopicPermnent同
				if (vbroker.unlistenTopicPermnent(topic, consumerId) == CONSTS.REVOKE_NOK) {
					allOk = false;
				}
			}
		} finally {
			lock.unlock();
		}

		if (allOk) {
			res = CONSTS.REVOKE_OK;
		}

		return res;
	}

	private void doMerge() {
		int repSize = repairedSize.get();
		if (repSize == 0) {
			return;
		} else {
			try {
				lock.lock();
				boolean add = validNodes.addAll(repairedNodes);
				if (add) {
					repairedNodes.clear();
					repairedSize.addAndGet(-repSize);
					
					validSize.addAndGet(repSize);
					invalidSize.addAndGet(-repSize);
				} else {
					logger.error("add repaired nodes error.");
				}
				
				if (invalidSize.get() == 0 && repairedSize.get() == 0)
					allConnected = true;
			} finally {
				lock.unlock();
			}
		}
	}

	public int sendQueue(String queueName, MQMessage msg, boolean isDurable) {
		int res = CONSTS.REVOKE_NOK;

		// move repairdNodes to validNodes
		doMerge();

		try {
			//lock.lock();
			int size = validSize.get();
			if (size == 0) {
				Global.get().setLastError(CONSTS.ERR_NO_VALID_NODES);
				return CONSTS.REVOKE_NOK;
			}
			
			int cnt = 0;
			boolean flag = false;
			
			LVarObject seed = rbSendSeedMap.get(queueName);
			if (seed == null) {
				seed = new LVarObject(0L);
				rbSendSeedMap.put(queueName, seed);
			}
			
			while (cnt++ < size) {
				int idx = (int) (size > 1 ? (seed.incAndGet() % size) : 0);
				String vbrokerId = validNodes.get(idx);
				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (!vbroker.isWritable()) {
					continue;
				} else {
					flag = true;
				}
	
				this.currentVBrokerID = vbroker.getVBrokerId();
				res = vbroker.sendQueue(queueName, msg, isDurable);
				if (res == CONSTS.REVOKE_NOK_NET_EXCEPTION) {
					processErrVBroker(vbroker, idx, false);
				}
				break;
			}
			
			if (!flag) {
				Global.get().setLastError(CONSTS.ERR_NO_VALID_WIRTABLE_NODES);
				return CONSTS.REVOKE_NOK;
			}
		} finally {
			//lock.unlock();
		}

		return res;
	}

	public int publishTopic(String topic, MQMessage msg, boolean isDurable) {
		int res = CONSTS.REVOKE_NOK;

		// move repairdNodes to validNodes
		doMerge();

		try {
			//lock.lock();
			int size = validSize.get();
			if (size == 0) {
				Global.get().setLastError(CONSTS.ERR_NO_VALID_NODES);
				return CONSTS.REVOKE_NOK;
			}
			
			int cnt = 0;
			boolean flag = false;
			
			LVarObject seed = rbSendSeedMap.get(topic);
			if (seed == null) {
				seed = new LVarObject(0L);
				rbSendSeedMap.put(topic, seed);
			}
			
			while (cnt++ < size) {
				int idx = (int) (size > 1 ? (seed.incAndGet() % size) : 0);
				String vbrokerId = validNodes.get(idx);
				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (!vbroker.isWritable()) {
					continue;
				} else {
					flag = true;
				}
	
				this.currentVBrokerID = vbroker.getVBrokerId();
				res = vbroker.publishTopic(topic, msg, isDurable);
				if (res == CONSTS.REVOKE_NOK_NET_EXCEPTION) {
					processErrVBroker(vbroker, idx, false);
				}
				break;
			}
			
			if (!flag) {
				Global.get().setLastError(CONSTS.ERR_NO_VALID_WIRTABLE_NODES);
				return CONSTS.REVOKE_NOK;
			}
		} finally {
			//lock.unlock();
		}

		return res;
	}
	
	public int publishTopicWildcard(String mainKey, String subKey, MQMessage msg, boolean isDurable) {
		int res = CONSTS.REVOKE_NOK;

		// move repairdNodes to validNodes
		doMerge();

		try {
			//lock.lock();
			int size = validSize.get();
			if (size == 0) {
				Global.get().setLastError(CONSTS.ERR_NO_VALID_NODES);
				return CONSTS.REVOKE_NOK;
			}
			
			int cnt = 0;
			boolean flag = false;
			
			LVarObject seed = rbSendSeedMap.get(subKey);
			if (seed == null) {
				seed = new LVarObject(0L);
				rbSendSeedMap.put(subKey, seed);
			}
			
			while (cnt++ < size) {
				int idx = (int) (size > 1 ? (seed.incAndGet() % size) : 0);
				String vbrokerId = validNodes.get(idx);
				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (!vbroker.isWritable()) {
					continue;
				} else {
					flag = true;
				}
	
				this.currentVBrokerID = vbroker.getVBrokerId();
				res = vbroker.publishTopicWildcard(mainKey, subKey, msg, isDurable);
				if (res == CONSTS.REVOKE_NOK_NET_EXCEPTION) {
					processErrVBroker(vbroker, idx, false);
				}
				break;
			}
			
			if (!flag) {
				Global.get().setLastError(CONSTS.ERR_NO_VALID_WIRTABLE_NODES);
				return CONSTS.REVOKE_NOK;
			}
		} finally {
			//lock.unlock();
		}

		return res;
	}

	public int consumeMessage(String name, MQMessage message, int timeout) {
		int res = 0;

		// move repairdNodes to validNodes
		doMerge();

		try {
			//lock.lock();
			int size = validSize.get();
			if (size == 0) {
				Global.get().setLastError(CONSTS.ERR_NO_VALID_NODES);
				return CONSTS.REVOKE_NOK;
			}
			
			int start = (int) (size > 1 ? (rbRevSeed++ % size) : 0);
			for (int cnt = 0; cnt < size; cnt++) {
				String vbrokerId = validNodes.get(start);
				VBroker vbroker = vbrokerMap.get(vbrokerId);

				int r = vbroker.consumeMessage(name, message, timeout);
				if (r == 1) {
					res = 1;
					break;
				} else if (r == 0) {
					start = ++start % size;
					rbRevSeed++;
				} else if (r == CONSTS.REVOKE_NOK_SHUNDOWN) {
					processErrVBroker(vbroker, start, false);
					break;
				} else {
					res = -1;
					break;
				}
			}
		} finally {
			//lock.unlock();
		}

		return res;
	}

	public int consumeMessageWildcard(String mainKey, String subKey, String consumerId, MQMessage message, int timeout) {
		int res = 0;

		// move repairdNodes to validNodes
		doMerge();

		try {
			//lock.lock();
			int size = validSize.get();
			if (size == 0) {
				Global.get().setLastError(CONSTS.ERR_NO_VALID_NODES);
				return CONSTS.REVOKE_NOK;
			}
			
			int start = (int) (size > 1 ? (rbRevSeed++ % size) : 0);

			for (int cnt = 0; cnt < size; cnt++) {
				String vbrokerId = validNodes.get(start);
				VBroker vbroker = vbrokerMap.get(vbrokerId);

				int r = vbroker.consumeMessageWildcard(mainKey, subKey, consumerId, message, timeout);
				if (r == 1) {
					res = 1;
					break;
				} else if (r == 0) {
					start = ++start % size;
					rbRevSeed++;
				} else if (r == CONSTS.REVOKE_NOK_SHUNDOWN) {
					processErrVBroker(vbroker, start, false);
					break;
				} else {
					res = -1;
					break;
				}
			}
		} finally {
			//lock.unlock();
		}

		return res;
	}
	
	private boolean processConnErr(VBroker vbroker) {
		boolean ret = false;
		
		try {
			startReconncetor();
			
			logger.error("VBroker caught error, {} need reconnect.", vbroker);
			reconnector.AddBrokenVBroker(vbroker.getVBrokerId(), vbroker.isNeedRepairImmediate());
			
			ret = true;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		
		return ret;
	}
	
	private boolean processErrVBroker(VBroker vbroker, int index, boolean isNeedSwitch) {

		vbroker.softcut();
		
		//目前暂时不做自动切换，防止主从同时挂掉时将主节点切到错误的节点上
		if (isNeedSwitch)
			vbroker.doSwitch();
		
		boolean ret = false;
		
		try {
			startReconncetor();
	
			validNodes.remove(index);
			invalidNodes.add(vbroker.getVBrokerId());
	
			invalidSize.incrementAndGet();
			validSize.decrementAndGet();
	
			logger.error("VBroker caught error, {} need reconnect.", vbroker);
			reconnector.AddBrokenVBroker(vbroker.getVBrokerId(), vbroker.isNeedRepairImmediate());
			
			allConnected = false;
			
			ret = true;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		
		return ret;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}
	
	public void cloneQueueDtlMap(HashMap<String, QueueDtlBean> queueDtlMap, String excludeVBrokerID) {
		if (validSize.get() <= 0) {
			try {
				lock.lock();
				
				for (int i = 0; i < invalidSize.get(); i++) {
					String invalidVBrokerId = invalidNodes.get(i);
					if (invalidVBrokerId.equals(excludeVBrokerID))
						continue;
					
					VBroker vbroker = vbrokerMap.get(invalidVBrokerId);
					if (vbroker == null)
						continue;
					
					vbroker.cloneBackupQueueDtlMap(queueDtlMap);
					if (queueDtlMap.size() > 0)
						break;
				}
			} finally {
				lock.unlock();
			}
			
			return;
		}
		
		try {
			lock.lock();
			String validVBrokerId = validNodes.get(0);
			VBroker vbroker = vbrokerMap.get(validVBrokerId);
			vbroker.cloneQueueDtlMap(queueDtlMap);
		} finally {
			lock.unlock();
		}	
	}
	
	public boolean doHaSwitch(String vbrokerId, String brokerId) {
		VBroker vbroker = vbrokerMap.get(vbrokerId);
		if (vbroker == null) {
			logger.error("VBrokerGroup doHaSwitch cannot map vbrokerId:{}", vbrokerId);
			return false;
		}
		
		if (vbroker.getMasterId().equals(brokerId))  // 防止重复触发
			return true;
		
		vbroker.setMasterId(brokerId);
		boolean ret = false;
		
		try {
			lock.lock();
			int idx = validNodes.indexOf(vbrokerId);
			if (idx != -1) {
				ret = processErrVBroker(vbroker, idx, false);   // 已经重置了最新的主节点id重连可以立即处理
			} else {
				if (invalidNodes.contains(vbrokerId)) {
					ret = reconnector.MoveMultiNodeVBroker(vbrokerId);
				}
			}
		} finally {
			lock.unlock();
		}
		
		return ret;
	}
	
	public boolean doExpansion(String jsonStr) {
		Map<String, VBroker> expandVBrokerMap = parseExpansionInfo(jsonStr);
		if (expandVBrokerMap == null)
			return false;
		
		logger.info("doExpansion %s", jsonStr);
		try {
			lock.lock();
			Set<Entry<String, VBroker>> entrySet = expandVBrokerMap.entrySet();
			for (Entry<String, VBroker> entry : entrySet) {
				String vbrokerId = entry.getKey();
				VBroker vbroker = entry.getValue();
				vbrokerMap.put(vbrokerId, vbroker);
				
				invalidNodes.add(vbrokerId);
				invalidSize.incrementAndGet();
				
				startReconncetor();
				reconnector.AddBrokenVBroker(vbrokerId, true);
			}
		} finally {
			lock.unlock();
		}
		
		return true;
	}
	
	public boolean doShrink(String vbrokerId) {
		boolean ret = false;
		
		try {
			lock.lock();
			if (!vbrokerMap.containsKey(vbrokerId))
				return false;
			
			if (validNodes.contains(vbrokerId)) {
				VBroker vbroker = vbrokerMap.remove(vbrokerId);
				if (vbroker != null) {
					validNodes.remove(vbrokerId);
					validSize.decrementAndGet();
					
					vbroker.close();
					vbroker.setWritable(true);
					ret = true;
				}
			}
			
			if (invalidNodes.contains(vbrokerId)) {
				reconnector.removeBrokenVBroker(vbrokerId);
				invalidNodes.remove(vbrokerId);
				invalidSize.decrementAndGet();
				
				ret = true;
			}
			
			if (repairedNodes.contains(vbrokerId)) {
				VBroker vbroker = vbrokerMap.remove(vbrokerId);
				if (vbroker != null) {
					repairedNodes.remove(vbrokerId);
					repairedSize.decrementAndGet();
					
					vbroker.close();
					vbroker.setWritable(true);
					ret = true;
				}
			}
		} finally {
			lock.unlock();
		}
		
		return ret;
	}
	
	public boolean doVBrokerStopWrite(String vbrokerId) {
		boolean ret = false;
		
		try {
			lock.lock();
			VBroker vbroker = vbrokerMap.get(vbrokerId);
			if (vbroker != null) {
				vbroker.setWritable(false);
			}
		} finally {
			lock.unlock();
		}
		
		return ret;
	}
	
	private Map<String, VBroker> parseExpansionInfo(String jsonStr) {
		JSONObject jsonRoot = JSON.parseObject(jsonStr);
		if (jsonRoot == null) {
			logger.error("Expansion json string illegal:{}", jsonStr);
			return null;
		}
		
		JSONArray jsonArr = (JSONArray) jsonRoot.get(CONSTS.JSON_HEADER_BROKERS);
		if (jsonArr == null) {
			logger.error("Expansion json string illegal, BROKERS item null:{}", jsonStr);
			return null;
		}
		
		int size = jsonArr.size();
		
		Map<String, VBroker> tmpMap = new HashMap<String, VBroker>();
		String groupId = "", groupName = "";
		
		for (int i = 0; i < size; i++) {
			JSONObject subJson = jsonArr.getJSONObject(i);
			
			if (subJson == null)
				continue;
			
			String brokerId = subJson.getString(CONSTS.JSON_HEADER_BROKERID);
			String brokerName = subJson.getString(CONSTS.JSON_HEADER_BROKERNAME);
			String vbrokerId = subJson.getString(CONSTS.JSON_HEADER_VBROKERID);
			String vbrokerName = subJson.getString(CONSTS.JSON_HEADER_VBROKERNAME);
			String hostName = subJson.getString(CONSTS.JSON_HEADER_HOSTNAME);
			String ip = subJson.getString(CONSTS.JSON_HEADER_IP);
			String vip = subJson.getString(CONSTS.JSON_HEADER_VIP);
			
			String sPort = subJson.getString(CONSTS.JSON_HEADER_PORT);
			String sMgrPort = subJson.getString(CONSTS.JSON_HEADER_MGRPORT);
			String mqUser = subJson.getString(CONSTS.JSON_HEADER_USER);
			String mqPwd = subJson.getString(CONSTS.JSON_HEADER_PASSWORD);
			String vhost = subJson.getString(CONSTS.JSON_HEADER_VHOST);

			String masterId = subJson.getString(CONSTS.JSON_HEADER_MASTER_ID);
			String erlCookie = subJson.getString(CONSTS.JSON_HEADER_ERL_COOKIE);
			
			String sCluster = subJson.getString(CONSTS.JSON_HEADER_CLUSTER);
			boolean bCluster = sCluster.equals(CONSTS.CLUSTER);
			
			String sWritable = subJson.getString(CONSTS.JSON_HEADER_WRITABLE);
			boolean bWritable = sWritable.equals(CONSTS.WRITABLE);
			
			groupId = subJson.getString(CONSTS.JSON_HEADER_GROUP_ID);
			groupName = subJson.getString(CONSTS.JSON_HEADER_GROUP_NAME);
			
			if (StringUtils.isNullOrEmtpy(brokerId) || StringUtils.isNullOrEmtpy(brokerName) || StringUtils.isNullOrEmtpy(vbrokerId)
					|| StringUtils.isNullOrEmtpy(vbrokerName) || StringUtils.isNullOrEmtpy(hostName) || StringUtils.isNullOrEmtpy(ip)
					|| StringUtils.isNullOrEmtpy(vip) || StringUtils.isNullOrEmtpy(sPort) || StringUtils.isNullOrEmtpy(sMgrPort)
					|| StringUtils.isNullOrEmtpy(mqUser) || StringUtils.isNullOrEmtpy(mqPwd) || StringUtils.isNullOrEmtpy(vhost)
					|| StringUtils.isNullOrEmtpy(masterId) || StringUtils.isNullOrEmtpy(erlCookie) || StringUtils.isNullOrEmtpy(sCluster)
					|| StringUtils.isNullOrEmtpy(groupId) || StringUtils.isNullOrEmtpy(groupName)) {
				logger.error("data illagel:" + jsonStr);
				return null;
			}
			
			int port = Integer.valueOf(sPort);
			int mgrPort = Integer.valueOf(sMgrPort);
			
			Broker broker = new Broker(brokerId, brokerName, ip, port, mqUser, mqPwd, vhost);
			
			if (tmpMap.containsKey(vbrokerId)) {
				VBroker vbroker = tmpMap.get(vbrokerId);
				vbroker.addBroker(broker);
			} else {
				HashMap<String, QueueDtlBean> queueDtlMap = new HashMap<String, QueueDtlBean>();
				cloneQueueDtlMap(queueDtlMap, "");
				
				VBroker vbroker = new VBroker(vbrokerId, vbrokerName, masterId, bWritable, queueDtlMap);
				vbroker.addBroker(broker);
				tmpMap.put(vbrokerId, vbroker);
			}
		}
		
		return tmpMap;
	}

	public void computeTPS() {
		try {
			lock.lock();
			for (String vbrokerId : validNodes) {
				if (StringUtils.isNullOrEmtpy(vbrokerId))
					continue;

				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (vbroker == null)
					continue;

				vbroker.computeTPS();
			}
		} finally {
			lock.unlock();
		}
	}

	public String getStatisticInfo() {
		try {
			lock.lock();
			if (rptStrBuilder.length() > 0)
				rptStrBuilder.delete(0, rptStrBuilder.length());
			int i = 0;

			for (String vbrokerId : validNodes) {
				if (StringUtils.isNullOrEmtpy(vbrokerId))
					continue;

				VBroker vbroker = vbrokerMap.get(vbrokerId);
				if (vbroker == null)
					continue;

				String vbRptStr = vbroker.getStatisticInfo();
				if (StringUtils.isNullOrEmtpy(vbRptStr))
					continue;

				if (i > 0)
					rptStrBuilder.append(CONSTS.COMMA);
				rptStrBuilder.append(vbRptStr);

				i++;
			}
		} finally {
			lock.unlock();
		}

		return rptStrBuilder.toString();
	}

}
