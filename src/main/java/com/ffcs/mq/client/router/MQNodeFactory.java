package com.ffcs.mq.client.router;

import com.ffcs.mq.client.rabbit.RabbitMQNode;
import com.ffcs.mq.client.utils.CONSTS;

public class MQNodeFactory {

	public IMQNode createMQNode(String type) {
		if (type.equals(CONSTS.MQ_TYPE_RABBITMQ)) {
			return new RabbitMQNode();
		}

		return null;
	}

}
