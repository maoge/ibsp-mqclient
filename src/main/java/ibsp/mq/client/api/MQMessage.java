package ibsp.mq.client.api;

import ibsp.common.utils.CONSTS;
import ibsp.mq.client.router.IMQNode;

public class MQMessage {
	// 消息体
	private byte[] body;

	// 消息id
	private String messageID;

	// 消息时间
	private long timeStamp;
	
	private int priority;

	// 队列类型 1:queue，2:topic
	private int sourceType;

	// 队列名或主题名称
	private String sourceName;

	// deliveryTagID
	private long deliveryTagID;

	private String consumerTag;
	private String realQueueName;
	private String consumerId;

	private IMQNode node;

	public MQMessage() {
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

	public String getMessageID() {
		return messageID;
	}

	public void setMessageID(String messageID) {
		this.messageID = messageID;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
	
	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		if (priority < CONSTS.MQ_DEFAULT_QUEUE_PRIORITY)
			this.priority = CONSTS.MQ_DEFAULT_QUEUE_PRIORITY;
		else if (priority > CONSTS.MQ_MAX_QUEUE_PRIORITY)
			this.priority = CONSTS.MQ_MAX_QUEUE_PRIORITY;
		else
			this.priority = priority;
	}

	public int getSourceType() {
		return sourceType;
	}

	public void setSourceType(int type) {
		this.sourceType = type;
	}

	public void setSourceName(String srcName) {
		this.sourceName = srcName;
	}

	public String getSourceName() {
		return sourceName;
	}

	public void clear() {
		timeStamp     = 0L;
		deliveryTagID = -1L;
		priority      = 0;
		body          = null;
		messageID     = null;
		sourceType    = -1;
		sourceName    = null;
		consumerTag   = null;
		realQueueName = null;
		consumerId    = null;
		node          = null;
	}

	public long getDeliveryTagID() {
		return deliveryTagID;
	}

	public void setDeliveryTagID(long deliveryTagID) {
		this.deliveryTagID = deliveryTagID;
	}

	public String getConsumerTag() {
		return consumerTag;
	}

	public void setConsumerTag(String consumerTag) {
		this.consumerTag = consumerTag;
	}

	public String getRealQueueName() {
		return realQueueName;
	}

	public void setRealQueueName(String realQueueName) {
		this.realQueueName = realQueueName;
	}

	public IMQNode getNode() {
		return node;
	}

	public void setNode(IMQNode node) {
		this.node = node;
	}

	public int getBodyLen() {
		return body == null ? 0 : body.length;
	}

	public String getConsumerId() {
		return consumerId;
	}

	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

}