package com.ffcs.mq.client.api;

import com.ffcs.mq.client.router.Router;
import com.ffcs.mq.client.utils.CONSTS;
import com.ffcs.mq.client.utils.Global;
import com.ffcs.mq.client.utils.SRandomGenerator;
import com.ffcs.mq.client.utils.StringUtils;
import com.ffcs.mq.client.utils.SysConfig;

/*
 * clients multiplex router inorder to reduce connection to broker.
 * N:mq.router.multiplexing.ratio
 * 
 * Client1 -------|
 * Client2 -------|
 * Client3 -------|-------Router
 *   ...     ...  |
 * ClientN -------|
 * 
 */

public class MQClientImpl implements IMQClient {
	
	private String clientID;
	private Router router = null;

	public MQClientImpl() {
		clientID = SRandomGenerator.genUUID();
		Global.get().registerClient(this);
	}

	@Override
	public void setAuthInfo(String userId, String userPwd) {
		if (StringUtils.isNullOrEmtpy(userId) || StringUtils.isNullOrEmtpy(userPwd)) {
			String err = String.format("user id or pwd is null or null string.");
			Global.get().setLastError(err);
			return;
		}
		
		SysConfig.get().setMqUserId(userId);
		SysConfig.get().setMqUserPwd(userPwd);
	}

	@Override
	public int connect(String queueName) {
		return router.connect(clientID, queueName) ? CONSTS.REVOKE_OK : CONSTS.REVOKE_NOK;
	}

	@Override
	public void close() {
		router.close(clientID);
	}
	
	@Override
	public void setQos(int qos) {
		if (qos <= 0) {
			String err = String.format("qos must > 0, you set value is %d.", qos);
			Global.get().setLastError(err);
			return;
		}
		
		Global.get().setQos(qos);
	}

	@Override
	public String GetLastErrorMessage() {
		return Global.get().getLastError();
	}

	@Override
	public int queueDeclare(String queueName, boolean durable, String groupId, int type) {
		return router.queueDeclare(queueName, durable, false, false, groupId, type);
	}
	
	@Override
	public int queueDeclare(String queueName, boolean durable, boolean ordered, String groupId, int type) {
		return router.queueDeclare(queueName, durable, ordered, false, groupId, type);
	}
	
	@Override
	public int queueDeclare(String queueName, boolean durable, boolean ordered, boolean priority, String groupId, int type) {
		return router.queueDeclare(queueName, durable, ordered, priority, groupId, type);
	}

	@Override
	public int queueDelete(String queueName) {
		return router.queueDelete(queueName);
	}

	@Override
	public int listenQueue(String queueName) {
		return router.listenQueue(clientID, queueName);
	}

	@Override
	public int unlistenQueue(String queueName) {
		return router.unlistenQueue(clientID, queueName);
	}

	@Override
	public int listenTopicAnonymous(String topic) {
		return router.listenTopicAnonymous(clientID, topic);
	}

	@Override
	public int unlistenTopicAnonymous(String topic) {
		return router.unlistenTopicAnonymous(clientID, topic);
	}

	@Override
	public int listenTopicPermnent(String topicName, String consumerId) {
		return router.listenTopicPermnent(clientID, topicName, consumerId);
	}

	@Override
	public int unlistenTopicPermnent(String consumerId) {
		return router.unlistenTopicPermnent(clientID, consumerId);
	}

	@Override
	public int listenTopicWildcard(String mainKey, String subKey, String consumerId) {
		return router.listenTopicWildcard(clientID, mainKey, subKey, consumerId);
	}

	@Override
	public int unlistenTopicWildcard(String consumerId) {
		return router.unlistenTopicWildcard(clientID, consumerId);
	}

	@Override
	public String genConsumerId() {
		return router.genConsumerId();
	}

	@Override
	public int logicQueueDelete(String consumerId) {
		return router.logicQueueDelete(consumerId);
	}

	@Override
	public int sendQueue(String queueName, MQMessage message) {
		return router.sendQueue(queueName, message);
	}

	@Override
	public int publishTopic(String topic, MQMessage message) {
		return router.publishTopic(topic, message);
	}

	@Override
	public int publishTopicWildcard(String mainKey, String subKey, MQMessage message) {
		return router.publishTopicWildcard(mainKey, subKey, message);
	}
	
	@Override
	public int consumeMessage(MQMessage message, int timeout) {
		return router.consumeMessage(clientID, message, timeout);
	}

	@Override
	public int consumeMessage(String name, MQMessage message, int timeout) {
		return router.consumeMessage(clientID, name, message, timeout);
	}

	@Override
	public int consumeMessageWildcard(String mainKey, String subKey, String consumerId, MQMessage message, int timeout) {
		return router.consumeMessageWildcard(clientID, mainKey, subKey, consumerId, message, timeout);
	}

	@Override
	public int ackMessage(MQMessage message) {
		return router.ackMessage(message);
	}

	@Override
	public int rejectMessage(MQMessage message, boolean requeue) {
		return router.rejectMessage(message, requeue);
	}
	
	@Override
	public int getMessageReady(String name) {
		return router.getMessageReady(name);
	}
	
	public int purgeQueue(String queueName) {
		return router.purgeQueue(queueName);
	}
	
	public int purgeTopic(String consumerId) {
		return router.purgeTopic(consumerId);
	}

	@Deprecated
	@Override
	public void startTx() {

	}

	@Deprecated
	@Override
	public void commitTx() {

	}
	
	public String getClientID() {
		return this.clientID;
	}
	
	public void setRouter(Router router) {
		this.router = router;
	}

}
