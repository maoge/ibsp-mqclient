package test;

import com.ffcs.mq.client.api.IMQClient;
import com.ffcs.mq.client.api.MQClientImpl;

public class WildcardTopicLogicDelete {

	public static void main(String[] args) {
		String consumerId = "ConID_HPPvbdsFirElIwe4";

		IMQClient mqClient = new MQClientImpl();
		mqClient.setAuthInfo("admin", "admin");
		mqClient.connect("TOPIC_TEST");
		int retDel = mqClient.logicQueueDelete(consumerId);

		String info = String.format("logic delete result:%d", retDel);
		System.out.println(info);
		
		mqClient.close();
	}

}
