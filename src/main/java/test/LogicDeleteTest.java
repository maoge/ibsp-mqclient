package test;

import com.ffcs.mq.client.api.IMQClient;
import com.ffcs.mq.client.api.MQClientImpl;
import com.ffcs.mq.client.utils.CONSTS;
import com.ffcs.mq.client.utils.PropertiesUtils;

public class LogicDeleteTest {

	public static void main(String[] args) {
		String confName = "test";
		String consumerID = PropertiesUtils.getInstance(confName).get("consumerId");
		
		IMQClient mqClient = new MQClientImpl();
		mqClient.setAuthInfo("admin", "admin");
		int ret = mqClient.logicQueueDelete(consumerID);
		if (ret == CONSTS.REVOKE_OK) {
			System.out.println("Logic Delete ok.");
		} else {
			System.out.println("Logic Delete fail:" + mqClient.GetLastErrorMessage());
		}
		
		mqClient.close();
	}

}
