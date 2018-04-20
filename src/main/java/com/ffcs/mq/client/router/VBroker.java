package com.ffcs.mq.client.router;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ffcs.mq.client.api.MQMessage;
import com.ffcs.mq.client.bean.ClientStatisticInfoBean;
import com.ffcs.mq.client.bean.QueueDtlBean;
import com.ffcs.mq.client.rabbit.RabbitMQNode;
import com.ffcs.mq.client.utils.CONSTS;
import com.ffcs.mq.client.utils.Global;
import com.ffcs.mq.client.utils.StringUtils;
import com.ffcs.mq.client.utils.SysConfig;

public class VBroker {

	private static Logger logger = LoggerFactory.getLogger(VBroker.class);

	private Map<String, Broker> brokerMap; // brokerId -> broker
	private Vector<String> brokerIds;

	private String vbrokerId;
	private String vbrokerName;
	private String masterId;      // 主节点的brokerId
	private boolean isWritable;
	
	private int nodeSize;

	private Map<String, ClientStatisticInfoBean> staticInfoMap;

	private StringBuilder statInfoStrBuilder;

	private IMQNode mqNode;

	public VBroker(String vbrokerId, String vbrokerName, String masterId, boolean isWritable) {
		this.vbrokerId   = vbrokerId;
		this.vbrokerName = vbrokerName;
		this.masterId    = masterId;
		this.isWritable  = isWritable;

		staticInfoMap    = new HashMap<String, ClientStatisticInfoBean>();

		brokerMap = new HashMap<String, Broker>();
		brokerIds = new Vector<String>();
		
		nodeSize = 0;

		statInfoStrBuilder = new StringBuilder();
	}

	public VBroker(String vbrokerId, String vbrokerName, String masterId, boolean isWritable,
			HashMap<String, QueueDtlBean> lsnrMap) {
		
		this(vbrokerId, vbrokerName, masterId, isWritable);
		
		String type = SysConfig.get().getMqType();
		if (type.equals(CONSTS.MQ_TYPE_RABBITMQ)) {
			mqNode = new RabbitMQNode(lsnrMap, this);
		}
	}
	
	public int connect() {
		int res = CONSTS.REVOKE_NOK;

		if (mqNode == null) {
			mqNode = new MQNodeFactory().createMQNode(SysConfig.get().getMqType());
			mqNode.setVBrokerInfo(this);

			res = mqNode.connect();
		} else {
			if (!mqNode.isConnect()) {
				res = mqNode.connect();
			}
		}

		if (mqNode.isConnect()) {
			res = CONSTS.REVOKE_OK;
		}

		Broker mBroker = brokerMap.get(masterId);
		if (mBroker != null) {
			logger.debug("connect:{} {}", mBroker, res == CONSTS.REVOKE_OK ? "success" : "fail");
		}

		return res;
	}

	public int close() {
		int res = CONSTS.REVOKE_NOK;

		if (mqNode != null) {
			res = mqNode.close();

			if (res == CONSTS.REVOKE_OK) {
				brokerMap.clear();
				brokerIds.clear();
			}
		}

		mqNode = null;
		return res;
	}

	// 关闭channel和connection, 与close不同点:但是保留原始监听的queue/topic信息方便连接修复后重新监听
	public int softcut() {
		int res = CONSTS.REVOKE_NOK;

		if (mqNode != null) {
			res = mqNode.softcut();
		}

		// mqNode = null;
		return res;
	}
	
	/**
	 * 当VBroker由于某些原因无法应答客户端时，不让客户端僵死等待，超时后强行把连接关闭
	 * @throws IOException 
	 */
	public int forceClose() throws IOException {
		int res = CONSTS.REVOKE_NOK;
		if (mqNode != null) {
			res = mqNode.forceClose();
		}
		return res;
	}

	public int relistenAfterBroken(HashMap<String, QueueDtlBean> queueDtlMap) {
		int res = CONSTS.REVOKE_NOK;

		if (mqNode != null && mqNode.isConnect()) {
			if (queueDtlMap != null)
				mqNode.PutBackUpConsumerMap(queueDtlMap);
			
			res = mqNode.relistenAfterBroken();
		}

		return res;
	}

	public int listenQueue(String queue) {
		int res = CONSTS.REVOKE_NOK;

		if (mqNode.isConnect()) {
			res = mqNode.listenQueue(queue);
		} else {
			Global.get().setLastError("mq node not connected.");
		}

		return res;
	}

	public int unlistenQueue(String queue) {
		int res = CONSTS.REVOKE_NOK;

		if (mqNode.isConnect()) {
			res = mqNode.unlistenQueue(queue);
		} else {
			Global.get().setLastError("mq node not connected.");
		}

		return res;
	}

	public int listenTopicAnonymous(String topic) {
		int res = CONSTS.REVOKE_NOK;

		if (mqNode.isConnect()) {
			res = mqNode.listenTopicAnonymous(topic);
		} else {
			Global.get().setLastError("mq node not connected.");
		}

		return res;
	}

	public int unlistenTopicAnonymous(String topic) {
		int res = CONSTS.REVOKE_NOK;

		if (mqNode.isConnect()) {
			res = mqNode.unlistenTopicAnonymous(topic);
		} else {
			Global.get().setLastError("mq node not connected.");
		}

		return res;
	}

	public int listenTopicPermnent(String topic, String realQueueName, String consumerId) {
		int res = CONSTS.REVOKE_NOK;

		if (mqNode.isConnect()) {
			res = mqNode.listenTopicPermnent(topic, realQueueName, consumerId);
		} else {
			Global.get().setLastError("mq node not connected.");
		}

		return res;
	}

	public int unlistenTopicPermnent(String topic, String consumerId) {
		int res = CONSTS.REVOKE_NOK;

		if (mqNode.isConnect()) {
			res = mqNode.unlistenTopicPermnent(topic, consumerId);
		} else {
			Global.get().setLastError("mq node not connected.");
		}

		return res;
	}

	public int createAndBind(String topic, String realQueueName) {
		int res = CONSTS.REVOKE_NOK;

		if (mqNode.isConnect()) {
			res = mqNode.createAndBind(topic, realQueueName);
		} else {
			Global.get().setLastError("mq node not connected.");
		}

		return res;
	}

	public int unBindAndDelete(String topic, String realQueueName) {
		int res = CONSTS.REVOKE_NOK;

		if (mqNode.isConnect()) {
			res = mqNode.unBindAndDelete(topic, realQueueName);
		} else {
			Global.get().setLastError("mq node not connected.");
		}

		return res;
	}

	public int sendQueue(String queueName, MQMessage msg, boolean isDurable) {
		int res = mqNode.sendQueue(queueName, msg, isDurable);
		if (res == CONSTS.REVOKE_OK) {
			collectStatisticInfo(queueName, msg, CONSTS.TYPE_PRO);
		}
		return res;
	}

	public int publishTopic(String topic, MQMessage msg, boolean isDurable) {
		int res = mqNode.publishTopic(topic, msg, isDurable);
		if (res == CONSTS.REVOKE_OK) {
			collectStatisticInfo(topic, msg, CONSTS.TYPE_PRO);
		}
		return res;
	}
	
	public int publishTopicWildcard(String mainKey, String subKey, MQMessage msg, boolean isDurable) {
		int res = mqNode.publishTopic(subKey, msg, isDurable);
		if (res == CONSTS.REVOKE_OK) {
			collectStatisticInfo(mainKey, msg, CONSTS.TYPE_PRO);
			collectStatisticInfo(subKey, msg, CONSTS.TYPE_PRO);
		}
		return res;
	}

	public int consumeMessage(String name, MQMessage message, int timeout) {
		int res = mqNode.consumeMessage(name, message, timeout);
		if (res == CONSTS.REVOKE_GETDATA) {
			collectStatisticInfo(name, message, CONSTS.TYPE_CON);
		}
		return res;
	}

	public int consumeMessageWildcard(String mainKey, String subKey, String consumerId, MQMessage message, int timeout) {
		int res = mqNode.consumeMessage(consumerId, message, timeout);
		if (res == CONSTS.REVOKE_GETDATA) {
			collectStatisticInfo(mainKey, message, CONSTS.TYPE_CON);
			collectStatisticInfo(subKey, message, CONSTS.TYPE_CON);
		}
		return res;
	}

	public boolean isConnect() {
		if (mqNode == null)
			return false;

		return mqNode.isConnect();
	}

	public void addBroker(Broker broker) {
		if (broker == null)
			return;

		String brokerId = broker.getBrokerId();
		if (brokerMap.containsKey(brokerId))
			return;

		brokerMap.put(brokerId, broker);
		brokerIds.add(brokerId);
		
		nodeSize++;
	}
	
	public boolean isNeedRepairImmediate() {
		return nodeSize <= 2;  // 单节点或2节点的镜像集群, 故障立即可连接重试
	}
	
	public void doSwitch() { 
		if (nodeSize != 2)
			return;
		
		for (String id : brokerIds) {
			if (!masterId.equals(id)) {
				masterId = id;
				
				break;
			}
		}
	}

	public String getVBrokerId() {
		return vbrokerId;
	}

	public void setVBrokerId(String vbrokerId) {
		this.vbrokerId = vbrokerId;
	}

	public String getVBrokerName() {
		return vbrokerName;
	}

	public void setVBrokerName(String vbrokerName) {
		this.vbrokerName = vbrokerName;
	}

	public String getMasterId() {
		return masterId;
	}

	public void setMasterId(String masterId) {
		this.masterId = masterId;
	}
	
	public boolean isWritable() {
		return isWritable;
	}

	public void setWritable(boolean isWritable) {
		this.isWritable = isWritable;
	}

	public Broker getMasterBroker() {
		if (StringUtils.isNullOrEmtpy(masterId)) {
			logger.error("masterId is null or emputy");
			return null;
		}

		Broker broker = brokerMap.get(masterId);
		return broker;
	}

	private void collectStatisticInfo(String name, MQMessage msg, int type) {
		ClientStatisticInfoBean infoBean = staticInfoMap.get(name);
		if (infoBean == null) {
			infoBean = new ClientStatisticInfoBean();
			infoBean.collectStatisticInfo(msg, type);
			staticInfoMap.put(name, infoBean);
		} else {
			infoBean.collectStatisticInfo(msg, type);
		}
	}

	public void computeTPS() {
		Set<Entry<String, ClientStatisticInfoBean>> entrySet = staticInfoMap.entrySet();
		for (Entry<String, ClientStatisticInfoBean> entry : entrySet) {
			String key = entry.getKey();
			ClientStatisticInfoBean statInfo = entry.getValue();
			if (statInfo == null) {
				logger.error("queue:{} binded ClientStatisticInfoBean is null.", key);
				continue;
			}

			statInfo.computeTPS();
		}
	}

	public String getStatisticInfo() {
		String localAddr = mqNode.getLocalAddr();
		String mqAddr = mqNode.getMQAddr();
		
		if (statInfoStrBuilder.length() > 0)
			statInfoStrBuilder.delete(0, statInfoStrBuilder.length());

		int i = 0;
		Set<Entry<String, ClientStatisticInfoBean>> entrySet = staticInfoMap.entrySet();
		for (Entry<String, ClientStatisticInfoBean> entry : entrySet) {
			String key = entry.getKey();
			ClientStatisticInfoBean statInfo = entry.getValue();
			if (statInfo == null) {
				logger.error("queue:{} binded ClientStatisticInfoBean is null.", key);
				continue;
			}

			int type = statInfo.getClientType();
			if (type == CONSTS.TYPE_NULL)
				continue;

			String strStatInfo = null;
			switch (type) {
			case CONSTS.TYPE_PRO:
				strStatInfo = String
						.format("{\"%s\":%d, \"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\", \"%s\":%d, \"%s\":%d, \"%s\":%d}",
								CONSTS.JSON_HEADER_CLIENT_TYPE, type, CONSTS.JSON_HEADER_QUEUE_NAME, key, CONSTS.JSON_HEADER_CLNT_IP_PORT,
								localAddr, CONSTS.JSON_HEADER_BKR_IP_PORT, mqAddr, CONSTS.JSON_HEADER_CLNT_PRO_TPS, statInfo.getProTPS(),
								CONSTS.JSON_HEADER_T_PRO_MSG_COUNT, statInfo.getProCnt(), CONSTS.JSON_HEADER_T_PRO_MSG_BYTES,
								statInfo.getProByte());
				break;
			case CONSTS.TYPE_CON:
				strStatInfo = String
						.format("{\"%s\":%d, \"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\", \"%s\":%d, \"%s\":%d, \"%s\":%d}",
								CONSTS.JSON_HEADER_CLIENT_TYPE, type, CONSTS.JSON_HEADER_QUEUE_NAME, key, CONSTS.JSON_HEADER_CLNT_IP_PORT,
								localAddr, CONSTS.JSON_HEADER_BKR_IP_PORT, mqAddr, CONSTS.JSON_HEADER_CLNT_CON_TPS, statInfo.getConTPS(),
								CONSTS.JSON_HEADER_T_CON_MSG_COUNT, statInfo.getConCnt(), CONSTS.JSON_HEADER_T_CON_MSG_BYTES,
								statInfo.getConByte());
				break;
			case CONSTS.TYPE_MIX:
				strStatInfo = String
						.format("{\"%s\":%d, \"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\", \"%s\":%d, \"%s\":%d, \"%s\":%d, \"%s\":%d, \"%s\":%d, \"%s\":%d}",
								CONSTS.JSON_HEADER_CLIENT_TYPE, type, CONSTS.JSON_HEADER_QUEUE_NAME, key, CONSTS.JSON_HEADER_CLNT_IP_PORT,
								localAddr, CONSTS.JSON_HEADER_BKR_IP_PORT, mqAddr, CONSTS.JSON_HEADER_CLNT_PRO_TPS, statInfo.getProTPS(),
								CONSTS.JSON_HEADER_T_PRO_MSG_COUNT, statInfo.getProCnt(), CONSTS.JSON_HEADER_T_PRO_MSG_BYTES,
								statInfo.getProByte(), CONSTS.JSON_HEADER_CLNT_CON_TPS, statInfo.getConTPS(),
								CONSTS.JSON_HEADER_T_CON_MSG_COUNT, statInfo.getConCnt(), CONSTS.JSON_HEADER_T_CON_MSG_BYTES,
								statInfo.getConByte());
				break;
			default:
				break;
			}

			if (strStatInfo == null)
				continue;
			if (i > 0)
				statInfoStrBuilder.append(CONSTS.COMMA);

			statInfoStrBuilder.append(strStatInfo);
			i++;
		}

		return statInfoStrBuilder.toString();
	}
	
	public void cloneQueueDtlMap(Map<String, QueueDtlBean> queueDtlMap) {
		mqNode.cloneQueueDtlMap(queueDtlMap);
	}
	
	public void cloneBackupQueueDtlMap(Map<String, QueueDtlBean> queueDtlMap) {
		mqNode.cloneBackupQueueDtlMap(queueDtlMap);
	}

	@Override
	public String toString() {
		return "VBroker [brokerMap=" + brokerMap + ", brokerIds=" + brokerIds + ", vbrokerId=" + vbrokerId + ", vbrokerName=" + vbrokerName
				+ ", masterId=" + masterId + ", isWritable=" + isWritable + ", mqNode=" + mqNode + "]";
	}

}
