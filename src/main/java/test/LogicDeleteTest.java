package test;

import com.ffcs.mq.client.api.IMQClient;
import com.ffcs.mq.client.api.MQClientImpl;
import com.ffcs.mq.client.utils.CONSTS;

public class LogicDeleteTest {

	public static void main(String[] args) {
		String consumerID = "ConID_Q45fVV00Xn13yeWs";
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
