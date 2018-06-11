package ibsp.mq.client.router;

import java.io.IOException;
import java.util.Map;

import ibsp.mq.client.api.MQMessage;
import ibsp.mq.client.bean.QueueDtlBean;

public interface IMQNode {

	public void setVBrokerInfo(VBroker vbroker);

	public VBroker getVBrokerInfo();

	public int connect();

	public int close();

	public int softcut(); // 与close不同在于保留需要监听的queue/topic信息, 方便重连上是重新监听用

	public int forceClose() throws IOException; //强行关闭到VBroker的连接
	
	public int relistenAfterBroken(); // 中断后重新监听

	public int sendQueue(String queue, MQMessage msg, boolean isDurable);

	public int publishTopic(String topic, MQMessage message, boolean isDurable);

	public int listenQueue(String queue);

	public int unlistenQueue(String queue);

	public int listenTopicAnonymous(String topic);

	public int unlistenTopicAnonymous(String topic);

	public int listenTopicPermnent(String topic, String realQueueName, String consumerId);

	public int unlistenTopicPermnent(String topic, String consumerId);

	public int createAndBind(String topic, String realQueueName); // 对应listenTopicPermernent
																	// 用于创建中转队列并和源队列绑定

	public int unBindAndDelete(String topic, String realQueueName); // createAndBind
																	// 反向动作

	public int consumeMessage(String name, MQMessage message, int timeout);

	public int ackMessage(MQMessage message);

	public int rejectMessage(MQMessage message, boolean requeue);

	public boolean isConnect();

	public String getLocalAddr();

	public String getMQAddr();
	
	public void cloneQueueDtlMap(Map<String, QueueDtlBean> cloneQueueDtlMap);
	public void cloneBackupQueueDtlMap(Map<String, QueueDtlBean> cloneQueueDtlMap);
	public void PutBackUpConsumerMap(Map<String, QueueDtlBean> cloneQueueDtlMap);

}
