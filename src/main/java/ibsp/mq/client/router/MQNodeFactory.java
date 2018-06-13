package ibsp.mq.client.router;

import ibsp.common.utils.CONSTS;
import ibsp.mq.client.rabbit.RabbitMQNode;

public class MQNodeFactory {

	public IMQNode createMQNode(String type) {
		if (type.equals(CONSTS.MQ_TYPE_RABBITMQ)) {
			return new RabbitMQNode();
		}

		return null;
	}

}
