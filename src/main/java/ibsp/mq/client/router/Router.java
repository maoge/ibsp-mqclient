package ibsp.mq.client.router;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import ibsp.mq.client.api.MQMessage;
import ibsp.mq.client.bean.ClientBindInfo;
import ibsp.mq.client.bean.LiteQueueBean;
import ibsp.mq.client.bean.ParseResultBean;
import ibsp.mq.client.event.EventMsg;
import ibsp.mq.client.event.EventType;
import ibsp.mq.client.utils.BasicOperation;
import ibsp.mq.client.utils.CONSTS;
import ibsp.mq.client.utils.Global;
import ibsp.mq.client.utils.IVarObject;
import ibsp.mq.client.utils.SRandomGenerator;
import ibsp.mq.client.utils.SVarObject;
import ibsp.mq.client.utils.StringUtils;

public class Router {

	private static Logger logger = LoggerFactory.getLogger(Router.class);
	
	private String routerID;
	
	private Map<String, VBrokerGroup> queue2group;       // queueName -> VBrokerGroup
	private Map<String, Queue> queueMap;                 // queueName -> Queue
	private Map<String, VBrokerGroup> groupMap;          // groupId -> VBrokerGroup
	private Map<String, IVarObject> listenMap;           // all listened queue or topic, value: bind client count
	private Map<String, ClientBindInfo> clientBindMap;   // clientID -> ClientBindInfo
	private Set<String> producerLockSet;                 // 全局有序队列是否加锁记录
	
	private StringBuilder reportStrBuilder;
	
	private ReentrantLock lock;
	
	private long sleepCnt = 0L;
	
	private long lastWriteTime = Long.MAX_VALUE; //最后一次写操作的时间(写超时用)
	private String currentGroupID = null; //目前在用的group
	

	public Router() {
		lock          = new ReentrantLock();
		
		routerID      = SRandomGenerator.genUUID();
		queue2group   = new HashMap<String, VBrokerGroup>();
		groupMap      = new HashMap<String, VBrokerGroup>();
		queueMap      = new HashMap<String, Queue>();
		listenMap     = new HashMap<String, IVarObject>();
		clientBindMap = new HashMap<String, ClientBindInfo>();
		producerLockSet = new HashSet<String>();
		
		reportStrBuilder = new StringBuilder();
	}
	
	public String getRouterID() {
		return this.routerID;
	}
	
	public long getLastWriteTime() {
		return this.lastWriteTime;
	}
	
	public void setLastWriteTime(long time) {
		lastWriteTime = time;
	}
	
	public VBroker getCurrentVBroker() {
		return this.groupMap.get(currentGroupID).getCurrentVBroker();
	}
	
	public int getBindSize() {
		int ret = 0;
		try {
			lock.lock();
			ret = clientBindMap.size();
		} finally {
			lock.unlock();
		}
		return ret;
	}
	
	public void addBindClient(String clientID) {
		try {
			lock.lock();
			if (!clientBindMap.containsKey(clientID)) {
				ClientBindInfo clientBindInfo = new ClientBindInfo(clientID);
				clientBindMap.put(clientID, clientBindInfo);
			}	
		} finally {
			lock.unlock();
		}
	}
	
	public void unbindClient(String clientID) {
		try {
			lock.lock();
			if (clientBindMap.containsKey(clientID)) {
				clientBindMap.remove(clientID);
			}
		} finally {
			lock.unlock();
		}
	}

	public boolean connect(String clientID, String queueName) {
		if (StringUtils.isNullOrEmtpy(queueName)) {
			String err = "queue name is null or emputy ......";
			logger.error(err);
			Global.get().setLastError(err);
			return false;
		}
		
		if (isQueueGroupExist(queueName)) {
			return true;   // avoid connect repeated, return direct.
		}
		
		boolean res = false;
		
		Queue queue = null;
		boolean queueExists = isQueueExist(queueName);
		if (!queueExists) {
			ParseResultBean queueParseResult = new ParseResultBean();
			queue = loadQueueByName(queueName, queueParseResult);

			if (!queueParseResult.isOk()) {
				logger.error(queueParseResult.getErrInfo());
				Global.get().setLastError(queueParseResult.getErrInfo());
				return false;
			}

			if (!queue.isDeployed()) {
				String err = String.format("queue:%s not deployed.", queueName);
				logger.error(err);
				Global.get().setLastError(err);
				return false;
			}
		} else {
			queue = getQueue(queueName);
		}
			
		try {
			lock.lock();
			VBrokerGroup vbGroup = null;
			
			boolean vbGroupExists = isGroupExist(queue.getGroupId());
			if (!vbGroupExists) {
				ParseResultBean vbGroupParseResult = new ParseResultBean();
				vbGroup = loadQueueBrokerRealtion(queueName, vbGroupParseResult);
	
				if (!vbGroupParseResult.isOk()) {
					logger.error(vbGroupParseResult.getErrInfo());
					Global.get().setLastError(vbGroupParseResult.getErrInfo());
					return false;
				}
			} else {
				vbGroup = getGroup(queue.getGroupId());
			}
			
			res = queue != null && vbGroup != null && queue.getGroupId().equals(vbGroup.getGroupId());
			if (res) {
				res = vbGroup.connect();
				if (res) {
					if (!queueExists)
						queueMap.put(queue.getQueueName(), queue);
						
					if (!vbGroupExists)
						groupMap.put(vbGroup.getGroupId(), vbGroup);
						
					queue2group.put(queue.getQueueName(), vbGroup);
					logger.info("connect:{} success.", queue.getQueueName());
				} else {
					logger.info("connect:{} fail.", queue.getQueueName());
				}
			}
		} finally {
			lock.unlock();
		}

		return res;
	}
	
	private boolean isQueueExist(String queueName) {
		boolean ret = false;
		
		try {
			lock.lock();
			ret = queueMap.containsKey(queueName);
		} finally {
			lock.unlock();
		}
		
		return ret;
	}
	
	private boolean isGroupExist(String groupId) {
		return groupMap.containsKey(groupId);
	}
	
	private boolean isQueueGroupExist(String queueName) {
		boolean ret = false;
		
		try {
			lock.lock();
			ret = queue2group.containsKey(queueName);
		} finally {
			lock.unlock();
		}
		
		return ret;
	}
	
	private Queue getQueue(String queueName) {
		Queue ret = null;
		
		try {
			lock.lock();
			ret = queueMap.get(queueName);
		} finally {
			lock.unlock();
		}
		
		return ret;
	}
	
	private VBrokerGroup getGroup(String groupId) {
		VBrokerGroup ret = null;
		
		try {
			lock.lock();
			ret = groupMap.get(groupId);
		} finally {
			lock.unlock();
		}
		
		return ret;
	}

	public void close(String clientID) {
		try {
			lock.lock();
			
			ClientBindInfo bindInfo = clientBindMap.get(clientID);
			if (bindInfo == null)
				return;
			
			List<LiteQueueBean> listenList = bindInfo.getListenList();
			if (listenList == null)
				return;
			
			clientBindMap.remove(clientID);
			
			for (LiteQueueBean bean : listenList) {
				String name = bean.getName();
				IVarObject var = listenMap.get(name);
				if (var == null)
					continue;
				
				if (var.decAndGet() == 0) {
					listenMap.remove(name);
				}
			}
			
			if (listenMap.size() > 0 || clientBindMap.size() > 0)
				return;
			
			Set<Entry<String, VBrokerGroup>> entrySet = groupMap.entrySet();
			for (Entry<String, VBrokerGroup> entry : entrySet) {
				VBrokerGroup vbGroup = entry.getValue();
				if (vbGroup == null)
					continue;

				vbGroup.close();
			}
			
			queue2group.clear();
			queueMap.clear();
			groupMap.clear();
			
			Global.get().removeRouter(routerID);
			Global.get().shutdown();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			lock.unlock();
		}
	}

	public int queueDeclare(String queueName, boolean durable, boolean ordered, String groupId, int type) {
		int res = CONSTS.REVOKE_NOK;

		SVarObject sVar = new SVarObject();
		String sType = type == CONSTS.TYPE_QUEUE ? "1" : "2";
		res = BasicOperation.queueDeclare(queueName, durable, ordered, false, groupId, sType, sVar);
		if (res != CONSTS.REVOKE_OK) {
			logger.error(sVar.getVal());
			Global.get().setLastError(sVar.getVal());
		}

		return res;
	}
	
	public int queueDeclare(String queueName, boolean durable, boolean ordered, boolean priority, String groupId, int type) {
		int res = CONSTS.REVOKE_NOK;

		SVarObject sVar = new SVarObject();
		String sType = type == CONSTS.TYPE_QUEUE ? "1" : "2";
		res = BasicOperation.queueDeclare(queueName, durable, ordered, priority, groupId, sType, sVar);
		if (res != CONSTS.REVOKE_OK) {
			logger.error(sVar.getVal());
			Global.get().setLastError(sVar.getVal());
		}

		return res;
	}

	public int queueDelete(String queueName) {
		int res = CONSTS.REVOKE_NOK;

		SVarObject sVar = new SVarObject();
		res = BasicOperation.queueDelete(queueName, sVar);
		if (res != CONSTS.REVOKE_OK) {
			logger.error(sVar.getVal());
			Global.get().setLastError(sVar.getVal());
		}

		return res;
	}
	
	private void incBindInfo(String clientID, int type, int topicType,
			String srcName, String mainKey, String subKey, String consumerID) {
		ClientBindInfo bindInfo = clientBindMap.get(clientID);
		if (bindInfo == null) {
			bindInfo = new ClientBindInfo(clientID);
			clientBindMap.put(clientID, bindInfo);
		}
		LiteQueueBean bean = new LiteQueueBean(type, topicType, srcName, mainKey, subKey, consumerID);
		bindInfo.addListen(bean);
		
		String name = bean.getName();
		IVarObject var = listenMap.get(name);
		if (var == null) {
			var = new IVarObject(1);
			listenMap.put(name, var);
			
			// Permnent/Wildcard topic consumerID -> VBrokerGroup
			VBrokerGroup group = queue2group.get(srcName);
			if (group != null) {
				queue2group.put(consumerID, group);
			}
		} else {
			var.incAndGet();
		}
	}
	
	private boolean decBindInfo(String clientID, String name) {
		boolean isListend = true;
		ClientBindInfo bindInfo = clientBindMap.get(clientID);
		if (bindInfo == null) {
			isListend = false;
		} else {
			isListend = bindInfo.isListened(name);
			if (isListend) {
				bindInfo.unlisten(name);
			}
		}
		return isListend;
	}
	
	private boolean decListenRef(String name) {
		boolean needUnlisten = false;
		IVarObject var = listenMap.get(name);
		if (var != null) {
			if (var.decAndGet() == 0) {
				listenMap.remove(name);
				needUnlisten = true;
			}
		}
		return needUnlisten;
	}

	public int listenQueue(String clientID, String queue) {
		int res = CONSTS.REVOKE_NOK;

		if (isQueueListened(queue)) {
			String info = String.format("queue:%s already listened.", queue);
			logger.info(info);
			
			try {
				lock.lock();
				incBindInfo(clientID, CONSTS.TYPE_QUEUE, CONSTS.TOPIC_DEFAULT, queue, queue, queue, null);
			} finally {
				lock.unlock();
			}
			
			return CONSTS.REVOKE_OK;
		}

		if (!isQueueLoaded(queue)) {
			return CONSTS.REVOKE_NOK;
		}

		try {
			lock.lock();
			
			// 全局有序队列排他
			Queue queueBean = getQueue(queue);
			if (queueBean.isOrdered()) {
				String lockName = getLockName(queue, CONSTS.TYPE_CON);
				if (!Global.get().orderedQueueLock(lockName)) {
					decBindInfo(clientID, queue);
					return CONSTS.REVOKE_NOK;
				}
			}
			
			VBrokerGroup vbGroup = queue2group.get(queue);
			if (vbGroup != null) {
				res = vbGroup.listenQueue(queue);
				if (res == CONSTS.REVOKE_OK) {
					incBindInfo(clientID, CONSTS.TYPE_QUEUE, CONSTS.TOPIC_DEFAULT, queue, queue, queue, null);
					
					String info = String.format("listenQueue:%s success.", queue);
					logger.info(info);
				} else {
					decBindInfo(clientID, queue);
					String err = String.format("listenQueue:%s fail.", queue);
					logger.error(err);
				}
			} else {
				String err = String.format("queue:%s not found, need connect first.", queue);
				logger.error(err);
			}
		} finally {
			lock.unlock();
		}

		return res;
	}

	public int unlistenQueue(String clientID, String queue) {
		int res = CONSTS.REVOKE_NOK;

		if (!isQueueListened(queue)) {
			String err = String.format("queue:%s not listened, do nothing.", queue);
			logger.error(err);
			Global.get().setLastError(err);
			return CONSTS.REVOKE_NOK;
		}

		if (!isQueueLoaded(queue)) {
			String err = String.format("queue:%s not loaded, do nothing.", queue);
			logger.error(err);
			Global.get().setLastError(err);
			return CONSTS.REVOKE_NOK;
		}

		try {
			lock.lock();
			
			VBrokerGroup vbGroup = queue2group.get(queue);
			if (vbGroup != null) {
				if (!decBindInfo(clientID, queue)) {
					String err = String.format("queue:%s not listened, do nothing.", queue);
					logger.error(err);
					Global.get().setLastError(err);
					return CONSTS.REVOKE_NOK;
				}
				
				if (decListenRef(queue)) {
					res = vbGroup.unlistenQueue(queue);
					if (res == CONSTS.REVOKE_OK) {
						// 全局有序需要清理锁
						Queue queueBean = getQueue(queue);
						if (queueBean.isOrdered()) {
							String lockName = getLockName(queue, CONSTS.TYPE_CON);
							if (!Global.get().orderedQueueUnlock(lockName)) {
								// 全局锁释放失败
								String err = String.format("ordered lock:{} release failed.", lockName);
								logger.error(err);
								return CONSTS.REVOKE_NOK;
							}
						}
						
						String info = String.format("unlistenQueue:%s success.", queue);
						logger.info(info);
					} else {
						String err = String.format("unlistenQueue:%s failed.", queue);
						logger.error(err);
					}
				}
			} else {
				String err = String.format("queue:%s not found, do nothing.", queue);
				logger.error(err);
			}
		} finally {
			lock.unlock();
		}

		return res;
	}

	public int listenTopicAnonymous(String clientID, String topic) {
		int res = CONSTS.REVOKE_NOK;

		if (isQueueListened(topic)) {
			String info = String.format("topic:%s already listened.", topic);
			logger.info(info);
			
			try {
				lock.lock();
				incBindInfo(clientID, CONSTS.TYPE_TOPIC, CONSTS.TOPIC_ANONYMOUS, topic, topic, topic, null);
			} finally {
				lock.unlock();
			}

			return CONSTS.REVOKE_OK;
		}
		
		if (!isQueueLoaded(topic)) {
			return CONSTS.REVOKE_NOK;
		}

		try {
			lock.lock();
			
			// 全局有序排他
			Queue queueBean = getQueue(topic);
			if (queueBean.isOrdered()) {
				String lockName = getLockName(topic, CONSTS.TYPE_CON);
				if (!Global.get().orderedQueueLock(lockName)) {
					decBindInfo(clientID, topic);
					return CONSTS.REVOKE_NOK;
				}
			}
			
			VBrokerGroup vbGroup = queue2group.get(topic);
			if (vbGroup != null) {
				res = vbGroup.listenTopicAnonymous(topic);
				if (res == CONSTS.REVOKE_OK) {
					incBindInfo(clientID, CONSTS.TYPE_TOPIC, CONSTS.TOPIC_ANONYMOUS, topic, null, null, null);
					
					String info = String.format("listen:%s success.", topic);
					logger.info(info);
				} else {
					decBindInfo(clientID, topic);
					String err = String.format("listen:%s failed.", topic);
					logger.error(err);
				}
			} else {
				String err = String.format("topic:%s not found, need connect first.", topic);
				logger.error(err);
				Global.get().setLastError(err);
			}
		} finally {
			lock.unlock();
		}

		return res;
	}

	public int unlistenTopicAnonymous(String clientID, String topic) {
		int res = CONSTS.REVOKE_NOK;

		if (!isQueueListened(topic)) {
			String err = String.format("topic:%s not listened, do nothing.", topic);
			logger.error(err);
			Global.get().setLastError(err);
			return CONSTS.REVOKE_NOK;
		}

		if (!isQueueLoaded(topic)) {
			String err = String.format("topic:%s not loaded, do nothing.", topic);
			logger.error(err);
			Global.get().setLastError(err);
			return CONSTS.REVOKE_NOK;
		}
		
		try {
			lock.lock();
			
			VBrokerGroup vbGroup = queue2group.get(topic);
			if (vbGroup != null) {
				if (!decBindInfo(clientID, topic)) {
					String err = String.format("topic:%s not listened, do nothing.", topic);
					logger.error(err);
					Global.get().setLastError(err);
					return CONSTS.REVOKE_NOK;
				}
				
				if (decListenRef(topic)) {
					res = vbGroup.unlistenTopicAnonymous(topic);
					if (res == CONSTS.REVOKE_OK) {
						// 全局有序需要清理锁
						Queue queueBean = getQueue(topic);
						if (queueBean.isOrdered()) {
							String lockName = getLockName(topic, CONSTS.TYPE_CON);
							if (!Global.get().orderedQueueUnlock(lockName)) {
								// 全局锁释放失败
								String err = String.format("ordered lock:{} release failed.", lockName);
								logger.error(err);
								return CONSTS.REVOKE_NOK;
							}
						}
						
						String info = String.format("unlistenTopicAnonymous:%s success.", topic);
						logger.info(info);
					} else {
						String err = String.format("unlistenTopicAnonymous:%s failed.", topic);
						logger.error(err);
					}
				}
			} else {
				String err = String.format("topic:%s not found, do nothing.", topic);
				logger.error(err);
			}
		} finally {
			lock.unlock();
		}

		return res;
	}

	public int listenTopicPermnent(String clientID, String topic, String consumerId) {
		int res = CONSTS.REVOKE_NOK;

		if (isQueueListened(consumerId)) {
			String info = String.format("topic:%s already listened.", topic);
			logger.info(info);
			
			try {
				lock.lock();
				incBindInfo(clientID, CONSTS.TYPE_TOPIC, CONSTS.TOPIC_PERMERNENT, topic, topic, topic, consumerId);
			} finally {
				lock.unlock();
			}
			
			return CONSTS.REVOKE_OK;
		}

		if (!isQueueLoaded(topic)) {
			return CONSTS.REVOKE_NOK;
		}
		
		try {
			lock.lock();
			
			// 全局有序排他
			Queue queueBean = getQueue(topic);
			if (queueBean.isOrdered()) {
				String lockName = getLockName(consumerId, CONSTS.TYPE_CON);
				if (!Global.get().orderedQueueLock(lockName)) {
					decBindInfo(clientID, consumerId);
					return CONSTS.REVOKE_NOK;
				}
			}
			
			VBrokerGroup vbGroup = queue2group.get(topic);
			Queue topicObj = queueMap.get(topic);
			if (vbGroup != null) {
				res = vbGroup.listenTopicPermnent(topicObj, consumerId);
				if (res == CONSTS.REVOKE_OK) {
					incBindInfo(clientID, CONSTS.TYPE_TOPIC, CONSTS.TOPIC_PERMERNENT, topic, topic, topic, consumerId);
					
					String info = String.format("listenTopicPermnent topic:%s consumerId:%s success.", topic, consumerId);
					logger.info(info);
				} else {
					decBindInfo(clientID, consumerId);
					String err = String.format("listenTopicPermnent topic:%s consumerId:%s failed.", topic, consumerId);
					logger.error(err);
				}
			} else {
				String err = String.format("topic:%s not found, need connect first.", topic);
				logger.error(err);
				Global.get().setLastError(err);
			}
		} finally {
			lock.unlock();
		}

		return res;
	}

	public int unlistenTopicPermnent(String clientID, String consumerId) {
		int res = CONSTS.REVOKE_NOK;

		SVarObject sVarRealQueue = new SVarObject();
		SVarObject sVarMainKey = new SVarObject();
		SVarObject sVarSubKey = new SVarObject();
		SVarObject sVarGroupId = new SVarObject();

		if (BasicOperation.getPermnentTopic(consumerId, sVarRealQueue, sVarMainKey, sVarSubKey, sVarGroupId) != CONSTS.REVOKE_OK) {
			return CONSTS.REVOKE_OK;
		}

		// listenTopicPermnent中mainKey和subKey填的都是topic
		String topic = sVarMainKey.getVal();
		
		if (!isQueueListened(consumerId)) {
			String err = String.format("topic:%s consumerId:%s not listened.", topic, consumerId);
			logger.error(err);
			Global.get().setLastError(err);
			return CONSTS.REVOKE_NOK;
		}
		
		if (!isQueueLoaded(topic)) {
			String err = String.format("topic:%s not loaded, do nothing.", topic);
			logger.error(err);
			Global.get().setLastError(err);
			return CONSTS.REVOKE_NOK;
		}
		
		try {
			lock.lock();
			
			VBrokerGroup vbGroup = queue2group.get(topic);
			if (vbGroup != null) {
				if (!decBindInfo(clientID, consumerId)) {
					String err = String.format("topic:%s consumerId:%s not listened, do nothing.", topic, consumerId);
					logger.error(err);
					Global.get().setLastError(err);
					return CONSTS.REVOKE_NOK;
				}
				
				if (decListenRef(consumerId)) {
					res = vbGroup.unlistenTopicPermnent(topic, consumerId);
					if (res == CONSTS.REVOKE_OK) {
						// 全局有序需要清理锁
						Queue queueBean = getQueue(topic);
						if (queueBean.isOrdered()) {
							String lockName = getLockName(consumerId, CONSTS.TYPE_CON);
							if (!Global.get().orderedQueueUnlock(lockName)) {
								// 全局锁释放失败
								String err = String.format("ordered lock:{} release failed.", lockName);
								logger.error(err);
								return CONSTS.REVOKE_NOK;
							}
						}
						
						String info = String.format("unlistenTopicPermnent:%s consumerId:%s success.", topic, consumerId);
						logger.info(info);
					} else {
						String err = String.format("unlistenTopicPermnent:%s consumerId:%s failed.", topic, consumerId);
						logger.error(err);
					}
				}
			} else {
				String err = String.format("topic:%s not found, do nothing.", topic);
				logger.error(err);
			}
		} finally {
			lock.unlock();
		}

		return res;
	}

	public int listenTopicWildcard(String clientID, String mainKey, String subKey, String consumerId) {
		int res = CONSTS.REVOKE_NOK;
		
		if (isQueueListened(consumerId)) {
			String info = String.format("mainKey:%s subKey%s consumerId:%s already listened.", mainKey, subKey, consumerId);
			logger.info(info);
			
			try {
				lock.lock();
				incBindInfo(clientID, CONSTS.TYPE_TOPIC, CONSTS.TOPIC_WILDCARD, mainKey, mainKey, subKey, consumerId);
			} finally {
				lock.unlock();
			}
			
			return CONSTS.REVOKE_NOK;
		}
		
		if (!isQueueLoaded(mainKey)) {
			return CONSTS.REVOKE_NOK;
		}
		
		try {
			lock.lock();
			
			// 全局有序排他
			Queue queueBean = getQueue(mainKey);
			if (queueBean.isOrdered()) {
				String lockName = getLockName(consumerId, CONSTS.TYPE_CON);
				if (!Global.get().orderedQueueLock(lockName)) {
					decBindInfo(clientID, consumerId);
					return CONSTS.REVOKE_NOK;
				}
			}
			
			VBrokerGroup vbGroup = queue2group.get(mainKey);
			Queue topicObj = queueMap.get(mainKey);
			if (vbGroup != null) {
				res = vbGroup.listenTopicWildcard(topicObj, subKey, consumerId);
				if (res == CONSTS.REVOKE_OK) {
					incBindInfo(clientID, CONSTS.TYPE_TOPIC, CONSTS.TOPIC_WILDCARD, mainKey, mainKey, subKey, consumerId);
					
					String info = String.format("listen success, mainKey:%s, subKey:%s.", mainKey, subKey);
					logger.info(info);
				} else {
					decBindInfo(clientID, consumerId);
					String err = String.format("listen failed, mainKey:%s, subKey:%s.", mainKey, subKey);
					logger.error(err);
				}
			} else {
				String err = String.format("topic:%s not found, need connect first.", mainKey);
				logger.error(err);
				Global.get().setLastError(err);
			}
		} finally {
			lock.unlock();
		}
		
		return res;
	}

	public int unlistenTopicWildcard(String clientID, String consumerId) {
		int res = CONSTS.REVOKE_NOK;

		SVarObject sVarRealQueue = new SVarObject();
		SVarObject sVarMainKey = new SVarObject();
		SVarObject sVarSubKey = new SVarObject();
		SVarObject sVarGroupId = new SVarObject();

		if (BasicOperation.getPermnentTopic(consumerId, sVarRealQueue, sVarMainKey, sVarSubKey, sVarGroupId) != CONSTS.REVOKE_OK) {
			return CONSTS.REVOKE_OK;
		}

		String mainKey = sVarMainKey.getVal();
		String subKey = sVarSubKey.getVal();
		
		if (!isQueueListened(consumerId)) {
			String err = String.format("mainKey:%s subKey:%s consumerId:%s not listened.", mainKey, subKey, consumerId);
			logger.error(err);
			Global.get().setLastError(err);
			return CONSTS.REVOKE_NOK;
		}
		
		try {
			lock.lock();
			
			VBrokerGroup vbGroup = queue2group.get(mainKey);
			if (vbGroup != null) {
				if (!decBindInfo(clientID, consumerId)) {
					String err = String.format("mainKey:%s subKey:%s consumerId:%s not listened, do nothing.", mainKey, subKey, consumerId);
					logger.error(err);
					Global.get().setLastError(err);
					return CONSTS.REVOKE_NOK;
				}
				
				if (decListenRef(consumerId)) {
					res = vbGroup.unlistenTopicWildcard(subKey, consumerId);
					if (res == CONSTS.REVOKE_OK) {
						// 全局有序需要清理锁
						Queue queueBean = getQueue(mainKey);
						if (queueBean.isOrdered()) {
							String lockName = getLockName(consumerId, CONSTS.TYPE_CON);
							if (!Global.get().orderedQueueUnlock(lockName)) {
								// 全局锁释放失败
								String err = String.format("ordered lock:{} release failed.", lockName);
								logger.error(err);
								return CONSTS.REVOKE_NOK;
							}
						}
						
						String info = String.format("unlistenTopicWildcard mainKey:%s subKey:%s consumerId:%s success.", mainKey, subKey, consumerId);
						logger.info(info);
					} else {
						String err = String.format("unlistenTopicWildcard mainKey:%s subKey:%s consumerId:%s failed.", mainKey, subKey, consumerId);
						logger.error(err);
					}
				}
			} else {
				String err = String.format("topic:%s not found, do nothing.", mainKey);
				logger.error(err);
			}
		} finally {
			lock.unlock();
		}

		return res;
	}

	public String genConsumerId() {
		String ret = null;

		SVarObject sVarID = new SVarObject();
		if (BasicOperation.genConsumerID(sVarID) == CONSTS.REVOKE_OK) {
			ret = sVarID.getVal();
		}

		return ret;
	}

	public int logicQueueDelete(String consumerId) {
		int res = CONSTS.REVOKE_NOK;

		SVarObject sVarRealQueue = new SVarObject();
		SVarObject sVarMainKey = new SVarObject();
		SVarObject sVarSubKey = new SVarObject();
		SVarObject sVarGroupId = new SVarObject();

		if (BasicOperation.getPermnentTopic(consumerId, sVarRealQueue, sVarMainKey, sVarSubKey, sVarGroupId) == CONSTS.REVOKE_OK) {

			String srcQueue = sVarMainKey.getVal();
			String realQueue = sVarRealQueue.getVal();
			String groupId = sVarGroupId.getVal();
			
			String tmpClientID = SRandomGenerator.genUUID();

			boolean isLoad = false;
			boolean needClose = false;
			if (!isQueueLoaded(srcQueue)) {
				if (connect(tmpClientID, srcQueue)) {
					isLoad = true;

					// 未连接过的VBrokerGroup unBindAndDelete完要close, 否则会有一组空连接限制着
					needClose = true;
				}
			} else {
				isLoad = true;
			}

			if (isLoad) {
				VBrokerGroup vbGroup = groupMap.get(groupId);
				if (vbGroup != null) {
					res = vbGroup.unBindAndDelete(consumerId, srcQueue, realQueue);
				} else {
					String err = String.format("groupId:%s not loaded.", groupId);
					logger.error(err);
				}

				if (needClose) {
					close(tmpClientID);
				}
			}
		} else {
			String err = String.format("consumerId[%s] not exists.", consumerId);
			Global.get().setLastError(err);
		}

		return res;
	}
	
	private boolean checkZKLock(String nodeName, int type) {
		String lockName = getLockName(nodeName, type);
		if (producerLockSet.contains(lockName))
			return true;
		
		if (Global.get().orderedQueueLock(lockName)) {
			producerLockSet.add(lockName);
			return true;
		}
		
		return false;
	}

	public int sendQueue(String queueName, MQMessage msg) {
		int res = CONSTS.REVOKE_NOK;
		
		try {
			lock.lock();
			this.lastWriteTime = System.currentTimeMillis();
			
			VBrokerGroup vbGroup = queue2group.get(queueName);
			if (vbGroup != null) {
				Queue queue = queueMap.get(queueName);
				boolean isDurable = queue.isDurable();
				boolean isOrdered = queue.isOrdered();
				if (isOrdered) {
					if (!checkZKLock(queueName, CONSTS.TYPE_PRO)) {
						String err = String.format("ordered queue:%s acquire lock fail.", queueName);
						logger.error(err);
						Global.get().setLastError(err);
						return CONSTS.REVOKE_NOK;
					}
				}

				this.currentGroupID = vbGroup.getGroupId();
				res = vbGroup.sendQueue(queueName, msg, isDurable);
			} else {
				String err = String.format("queue:%s not found, need connect first", queueName);
				logger.error(err);
				Global.get().setLastError(err);
			}
		} finally {
			this.lastWriteTime = Long.MAX_VALUE;
			lock.unlock();
		}

		return res;
	}

	public int publishTopic(String topic, MQMessage msg) {
		int res = CONSTS.REVOKE_NOK;
		
		try {
			lock.lock();
			this.lastWriteTime = System.currentTimeMillis();
			
			VBrokerGroup vbGroup = queue2group.get(topic);
			if (vbGroup != null) {
				Queue queue = queueMap.get(topic);
				boolean isDurable = queue.isDurable();
				boolean isOrdered = queue.isOrdered();
				if (isOrdered) {
					if (!checkZKLock(topic, CONSTS.TYPE_PRO)) {
						String err = String.format("ordered topic:%s acquire lock fail.", topic);
						logger.error(err);
						Global.get().setLastError(err);
						return CONSTS.REVOKE_NOK;
					}
				}

				this.currentGroupID = vbGroup.getGroupId();
				res = vbGroup.publishTopic(topic, msg, isDurable);
			} else {
				String err = String.format("topic:%s not found, need connect first.", topic);
				logger.error(err);
				Global.get().setLastError(err);
			}
		} finally {
			this.lastWriteTime = Long.MAX_VALUE;
			lock.unlock();
		}

		return res;
	}

	public int publishTopicWildcard(String mainKey, String subKey, MQMessage message) {
		int res = CONSTS.REVOKE_NOK;
		
		try {
			lock.lock();
			this.lastWriteTime = System.currentTimeMillis();
			
			VBrokerGroup vbGroup = queue2group.get(mainKey);
			if (vbGroup != null) {
				Queue queue = queueMap.get(mainKey);
				boolean isDurable = queue.isDurable();
				boolean isOrdered = queue.isOrdered();
				if (isOrdered) {
					if (!checkZKLock(subKey, CONSTS.TYPE_PRO)) {
						String err = String.format("ordered topic main_key:%s sub_key:%s acquire lock fail.", mainKey, subKey);
						logger.error(err);
						Global.get().setLastError(err);
						return CONSTS.REVOKE_NOK;
					}
				}

				this.currentGroupID = vbGroup.getGroupId();
				res = vbGroup.publishTopicWildcard(mainKey, subKey, message, isDurable);
			} else {
				String err = String.format("topic:%s not found, need connect first.", mainKey);
				logger.error(err);
				Global.get().setLastError(err);
			}
		} finally {
			this.lastWriteTime = Long.MAX_VALUE;
			lock.unlock();
		}

		return res;
	}
	
	public int consumeMessage(String clientID, MQMessage message, int timeout) {
		ClientBindInfo bindInfo = clientBindMap.get(clientID);
		if (bindInfo == null || bindInfo.listenSize() == 0) {
			logger.error(CONSTS.ERR_NO_QUEUE_LISTENED);
			Global.get().setLastError(CONSTS.ERR_NO_QUEUE_LISTENED);
			
			return CONSTS.REVOKE_NOK;
		}
		
		return consumeMessage(clientID, bindInfo.getNextConsumeQueue(), message, timeout);
	}

	public int consumeMessage(String clientID, String name, MQMessage message, int timeout) {
		int res = 0;
		
		try {
			lock.lock();
			
			VBrokerGroup vbGroup = queue2group.get(name);
			if (vbGroup != null) {
				res = vbGroup.consumeMessage(name, message, timeout);
			} else {
				String err = String.format("queue2group not find:%s, need connect(%s) first", name, name);
				logger.error(err);
				Global.get().setLastError(err);
			}
		} finally {
			lock.unlock();
		}
		
		if (res == 0) {
			consumeSleepWhenNoData(timeout);
		}

		return res;
	}

	public int consumeMessageWildcard(String clientID, String mainKey, String subKey, String consumerId, MQMessage message, int timeout) {
		int res = CONSTS.REVOKE_NOK;
		
		try {
			lock.lock();
			
			VBrokerGroup vbGroup = queue2group.get(mainKey);
			if (vbGroup != null) {
				res = vbGroup.consumeMessageWildcard(mainKey, subKey, consumerId, message, timeout);
			} else {
				String err = String.format("queue2group not find:%s, need connect(%s) first", mainKey, mainKey);
				logger.error(err);
				Global.get().setLastError(err);
			}
		} finally {
			lock.unlock();
		}

		if (res == 0) {
			consumeSleepWhenNoData(timeout);
		}
		
		return res;
	}

	public int ackMessage(MQMessage message) {
		int res = CONSTS.REVOKE_NOK;
		
		try {
			lock.lock();
			
			IMQNode node = message.getNode();
			if (node != null) {
				res = node.ackMessage(message);
				
				if (res == CONSTS.REVOKE_OK) {
					message.setNode(null);
				}
			} else {
				logger.error("message node is null, ack fail.");
			}
		} finally {
			lock.unlock();
		}

		return res;
	}

	public int rejectMessage(MQMessage message, boolean requeue) {
		int res = CONSTS.REVOKE_NOK;
		
		try {
			lock.lock();
			
			IMQNode node = message.getNode();
			if (node != null) {
				res = node.rejectMessage(message, requeue);
				
				if (res == CONSTS.REVOKE_OK) {
					message.setNode(null);
				}
			} else {
				logger.error("message node is null, reject fail.");
			}
		} finally {
			lock.unlock();
		}

		return res;
	}
	
	public int purgeQueue(String queueName) {
		int res = CONSTS.REVOKE_NOK;
		
		try {
			lock.lock();
			
			SVarObject sVar = new SVarObject();
			res = BasicOperation.revokePurgeQueue(queueName, CONSTS.TYPE_QUEUE, sVar);
			if (res != CONSTS.REVOKE_OK) {
				logger.error(sVar.getVal());
			}
		} finally {
			lock.unlock();
		}
		
		return res;
	}
	
	public int purgeTopic(String consumerId) {
		int res = CONSTS.REVOKE_NOK;
		
		try {
			lock.lock();
			
			SVarObject sVar = new SVarObject();
			res = BasicOperation.revokePurgeQueue(consumerId, CONSTS.TYPE_TOPIC, sVar);
			if (res != CONSTS.REVOKE_OK) {
				logger.error(sVar.getVal());
			}
		} finally {
			lock.unlock();
		}
		
		return res;
	}
	
	public int getMessageReady(String name) {
		return BasicOperation.getMessageReady(name);
	}

	private Queue loadQueueByName(String queueName, ParseResultBean result) {
		if (queueMap.containsKey(queueName)) {
			logger.info("queue:{} have loaded ......", queueName);

			result.setOk(true);
			return queueMap.get(queueName);
		}

		SVarObject sVar = new SVarObject();
		boolean ret = BasicOperation.loadQueueByName(queueName, sVar);
		if (!ret) {
			result.setOk(false);
			result.setErrInfo(Global.get().getLastError());
			return null;
		}
		
		JSONObject object = JSONObject.parseObject(sVar.getVal()).
				getJSONObject(CONSTS.JSON_HEADER_RET_INFO);
		Queue queue = Queue.fromJson(object);
		if (queue == null) {
			result.setOk(false);
			result.setErrInfo("loadQueueByName json:" + sVar.getVal() + " parse error ......");
			return null;
		} else {
			result.setOk(true);
		}

		return queue;
	}

	public VBrokerGroup loadQueueBrokerRealtion(String queueName, ParseResultBean result) {
		if (queue2group.containsKey(queueName)) {
			logger.info("queue:{} have loaded ......", queueName);

			VBrokerGroup vbGroup = queue2group.get(queueName);

			if (vbGroup != null) {
				result.setOk(true);
				return vbGroup;
			}
		}

		SVarObject sVar = new SVarObject();
		if (!BasicOperation.loadQueueBrokerRealtion(queueName, sVar)) {
			logger.error("HTTP GET loadQueueBrokerRealtion error ......", sVar.getVal());

			result.setOk(false);
			result.setErrInfo("HTTP GET loadQueueBrokerRealtion error ......");
			return null;
		}

		// do json parse
		JSONObject object = JSONObject.parseObject(sVar.getVal()).getJSONObject(CONSTS.JSON_HEADER_RET_INFO);
		VBrokerGroup vbGroup = parseVBrokerGroupJson(object, result);
		if (!result.isOk()) {
			return null;
		}

		return vbGroup;
	}

	private VBrokerGroup parseVBrokerGroupJson(JSONObject vbGroupObj, ParseResultBean result) {
		
		if (vbGroupObj == null) {
			result.setOk(false);
			result.setErrInfo("loadQueueBrokerRealtion json illegal");
			return null;
		}
		try {
			VBrokerGroup vbGroup = new VBrokerGroup(vbGroupObj.getString(CONSTS.JSON_HEADER_ID), 
					vbGroupObj.getString(CONSTS.JSON_HEADER_NAME));
		
			JSONArray vbrokerList = vbGroupObj.getJSONArray("VBROKERS");
			for (int i=0; i<vbrokerList.size(); i++) {
				JSONObject vbrokerObj = vbrokerList.getJSONObject(i);
				VBroker vbroker = new VBroker(vbrokerObj.getString(CONSTS.JSON_HEADER_ID),
						vbrokerObj.getString(CONSTS.JSON_HEADER_NAME),
						vbrokerObj.getString(CONSTS.JSON_HEADER_MASTER_ID),
						vbrokerObj.getBoolean(CONSTS.JSON_HEADER_WRITABLE));
			
				JSONArray brokerList = vbrokerObj.getJSONArray("BROKERS");
				for (int j=0; j<brokerList.size(); j++) {
					JSONObject brokerObj = brokerList.getJSONObject(j);
					Broker broker = new Broker(brokerObj.getString(CONSTS.JSON_HEADER_ID), 
							brokerObj.getString(CONSTS.JSON_HEADER_NAME), 
							brokerObj.getString(CONSTS.JSON_HEADER_IP),
							Integer.parseInt(brokerObj.getString(CONSTS.JSON_HEADER_PORT)),
							CONSTS.MQ_DEFAULT_USER,
							CONSTS.MQ_DEFAULT_PWD,
							CONSTS.MQ_DEFAULT_VHOST);
					vbroker.addBroker(broker);
				}	
				vbGroup.addToInvalidNodes(vbroker);
			}
			
			result.setOk(true);
			return vbGroup;
			
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			result.setOk(false);
			result.setErrInfo("Parse vbroker group error, "+e.getMessage());
			return null;
		}
	}

	private boolean isQueueListened(String queueName) {
		boolean ret = false;
		
		try {
			lock.lock();
			
			IVarObject var = listenMap.get(queueName);
			if (var != null) {
				ret = var.getVal() > 0;
			}
		} finally {
			lock.unlock();
		}

		return ret;
	}

	private boolean isQueueLoaded(String queueName) {
		boolean ret = false;
		
		try {
			lock.lock();
			
			Queue queue = queueMap.get(queueName);
			boolean bQueueLoaded = queue != null;
			String groupId = null;

			if (!bQueueLoaded) {
				String err = String.format("need connect(%s) first.", queueName);
				logger.error(err);
				Global.get().setLastError(err);

				return false;
			} else {
				groupId = queue.getGroupId();
			}

			if (groupMap.containsKey(groupId)) {
				ret = true;
			} else {
				String err = String.format("need connect(%s) first.", queueName);
				logger.error(err);
				Global.get().setLastError(err);

				return false;
			}

		} finally {
			lock.unlock();
		}

		return ret;
	}
	
	public String getLockName(String queueName, int type) {
		return String.format("%s:%s", queueName, type == CONSTS.TYPE_CON ? CONSTS.CONSUMER : CONSTS.PRODUCER);
	}
	
	public void doCompute() {
		if (groupMap == null)
			return;
		
		Set<Entry<String, VBrokerGroup>> entrySet = groupMap.entrySet();
		for (Entry<String, VBrokerGroup> entry : entrySet) {
			VBrokerGroup vbGroup = entry.getValue();
			if (vbGroup == null) {
				continue;
			}

			vbGroup.computeTPS();
		}
	}
	
	public void doReport() {
		if (groupMap == null)
			return;
		
		if (reportStrBuilder.length() > 0)
			reportStrBuilder.delete(0, reportStrBuilder.length());
		reportStrBuilder.append(CONSTS.SBRACKET_LEFT);

		int i = 0;

		Set<Entry<String, VBrokerGroup>> entrySet = groupMap.entrySet();
		for (Entry<String, VBrokerGroup> entry : entrySet) {
			VBrokerGroup vbGroup = entry.getValue();
			if (vbGroup == null) {
				continue;
			}

			String vbGrpRptInfo = vbGroup.getStatisticInfo();
			if (StringUtils.isNullOrEmtpy(vbGrpRptInfo))
				continue;

			if (i > 0)
				reportStrBuilder.append(CONSTS.COMMA);
			reportStrBuilder.append(vbGrpRptInfo);

			i++;
		}

		reportStrBuilder.append(CONSTS.SBRACKET_RIGHT);

		// "[]" not report
		if (reportStrBuilder.length() > 2) {
			String context = reportStrBuilder.toString();
			Global global = Global.get();
			String lsnrAddr = String.format("%s:%d", global.getLsnrIP(), global.getLsnrPort());
			int res = BasicOperation.putClientStatisticInfo(context, lsnrAddr);
			if (res != CONSTS.REVOKE_OK) {
				logger.error("client statistic info send to mcenter error.");
			} else {
				logger.debug("client statistic info send to mcenter ok.");
			}
		}
	}
	
	public boolean dispachEventMsg(EventMsg event) {
		if (event == null)
			return false;
		
		int evCode       = event.getEventCode();
		String groupId   = event.getGroupId();
		String vbrokerId = event.getVBrokerId();
		String brokerId  = event.getBrokerId();
		String jsonStr   = event.getJsonStr();
		
		boolean ret = false;
		
		try {
			lock.lock();
			
			VBrokerGroup vbGroup = groupMap.get(groupId);
			if (vbGroup == null) {
				logger.error("event process error, can not match group:{}", groupId);
				return false;
			}
		
			EventType evType = EventType.get(evCode);
			switch (evType) {
			case e51:              // stop write on vbroker
				ret = vbGroup.doVBrokerStopWrite(vbrokerId);
				break;
			case e52:              // group expansion
				ret = vbGroup.doExpansion(jsonStr);
				break;
			case e53:              // group shrink
				ret = vbGroup.doShrink(vbrokerId);
				break;
			case e56:              // ha switch event
				ret = vbGroup.doHaSwitch(vbrokerId, brokerId);
				break;
			default:
				break;
			}
		} finally {
			lock.unlock();
		}
		
		return ret;
	}
	
	private void consumeSleepWhenNoData(int timeout) {
		if (sleepCnt++ % CONSTS.CONSUME_BATCH_SLEEP_CNT == 0) {
			try {
				TimeUnit.MILLISECONDS.sleep(timeout <= 0 ? CONSTS.SLEEP_WHEN_NODATA : timeout);
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}				
			
			sleepCnt = 0L;
		}
	}

}
