package ibsp.mq.client.rabbit;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ReturnListener;

import ibsp.common.utils.BlockingLock;

public class RabbitRetListener implements ReturnListener {

	private String replyText;

	private volatile BlockingLock<Integer> lock;

	public RabbitRetListener(BlockingLock<Integer> lock) {
		this.lock = lock;
	}

	@Override
	public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties,
			byte[] body) throws IOException {
		this.replyText = replyText;
		this.lock.set(replyCode);
	}

	public String getReplyText() {
		return this.replyText;
	}

}
