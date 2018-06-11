package ibsp.mq.client.bean;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

public class QueueDtlBean {

	private String srcQueueName;        // name of queue or topic
	private int type;                   // queue:1; topic:2
	private int topicType;              // 当type = 2 时, 区分匿名还是永久, Anonymous:1;Permernent:2;Wildcard:3
	private String realQueueName;       // 当type = "2" 时, rabbit随机生成的队列名; type = "1"
									    // 时, realQueueName = srcQueueName;
	private QueueingConsumer consumer;  // 每个队列对应一个Channel
										// 同时绑定自己Channel生成的Consumer
	private String consumerTag;         // consumer tag
	private Channel dataChannel;        // 每个队列对应一个Channel
	private int channelIdx;             // channel index
	private String consumerId;          // topic permnent 方式对应的consumerId

	public QueueDtlBean(String srcQueueName, int type, int topicType, String realQueueName, String consumerTag,
			QueueingConsumer consumer, Channel dataChannel, int channelIdx, String consumerId) {
		this.srcQueueName = srcQueueName;
		this.type = type;
		this.topicType = topicType;
		this.realQueueName = realQueueName;
		this.consumerTag = consumerTag;
		this.consumer = consumer;
		this.dataChannel = dataChannel;
		this.channelIdx = channelIdx;
		this.consumerId = consumerId;
	}

	public String getSrcQueueName() {
		return srcQueueName;
	}

	public void setSrcQueueName(String srcQueueName) {
		this.srcQueueName = srcQueueName;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getTopicType() {
		return topicType;
	}

	public void setTopicType(int topicType) {
		this.topicType = topicType;
	}

	public String getRealQueueName() {
		return realQueueName;
	}

	public void setRealQueueName(String realQueueName) {
		this.realQueueName = realQueueName;
	}

	public QueueingConsumer getConsumer() {
		return consumer;
	}

	public void setConsumer(QueueingConsumer consumer) {
		this.consumer = consumer;
	}

	public String getConsumerTag() {
		return consumerTag;
	}

	public void setConsumerTag(String consumerTag) {
		this.consumerTag = consumerTag;
	}

	public Channel getDataChannel() {
		return dataChannel;
	}

	public void setDataChannel(Channel dataChannel) {
		this.dataChannel = dataChannel;
	}

	public int getChannelIdx() {
		return channelIdx;
	}

	public void setChannelIdx(int channelIdx) {
		this.channelIdx = channelIdx;
	}

	public String getConsumerId() {
		return consumerId;
	}

	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

}
