package test;

import com.ffcs.mq.client.api.IMQClient;
import com.ffcs.mq.client.api.MQClientImpl;

public class PermnentTopicLogicDelete {

	public static void main(String[] args) {
		String consumerId = "ConID_Q45fVV00Xn13yXnw";

		IMQClient mqClient = new MQClientImpl();
		int retDel = mqClient.logicQueueDelete(consumerId);

		String info = String.format("logic delete result:%d", retDel);
		System.out.println(info);
	}

}
