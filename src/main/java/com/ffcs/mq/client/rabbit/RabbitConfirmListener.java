package com.ffcs.mq.client.rabbit;

import java.io.IOException;

import com.ffcs.mq.client.utils.BlockingLock;
import com.rabbitmq.client.ConfirmListener;

public class RabbitConfirmListener implements ConfirmListener {

	private volatile BlockingLock<Integer> lock;

	public RabbitConfirmListener(BlockingLock<Integer> lock) {
		this.lock = lock;
	}

	@Override
	public void handleAck(long deliveryTag, boolean multiple) throws IOException {
		lock.set(0);
	}

	@Override
	public void handleNack(long deliveryTag, boolean multiple) throws IOException {

	}

}
