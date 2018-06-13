package test;

import ibsp.common.utils.CONSTS;
import ibsp.common.utils.PropertiesUtils;
import ibsp.mq.client.api.IMQClient;
import ibsp.mq.client.api.MQClientImpl;

public class LogicDeleteTest {

	public static void main(String[] args) {
		String confName = "test";
		String consumerID = PropertiesUtils.getInstance(confName).get("consumerId");
		String userName = PropertiesUtils.getInstance(confName).get("userName");
		String userPwd = PropertiesUtils.getInstance(confName).get("userPwd");
		
		IMQClient mqClient = new MQClientImpl();
		mqClient.setAuthInfo(userName, userPwd);
		int ret = mqClient.logicQueueDelete(consumerID);
		if (ret == CONSTS.REVOKE_OK) {
			System.out.println("Logic Delete ok.");
		} else {
			System.out.println("Logic Delete fail:" + mqClient.GetLastErrorMessage());
		}
		
		mqClient.close();
	}

}
