package ibsp.mq.client.router;

import ibsp.mq.client.rabbit.RabbitMQNode;
import ibsp.mq.client.utils.CONSTS;

public class MQNodeFactory {

	public IMQNode createMQNode(String type) {
		if (type.equals(CONSTS.MQ_TYPE_RABBITMQ)) {
			return new RabbitMQNode();
		}

		return null;
	}

}
