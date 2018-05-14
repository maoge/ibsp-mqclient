package test;

import com.ffcs.mq.client.api.IMQClient;
import com.ffcs.mq.client.api.MQClientImpl;
import com.ffcs.mq.client.utils.PropertiesUtils;

public class CreateQueueTest {
	
	public static void main(String[] args) {
		String confName = "test";
		String queueNamePrefix = PropertiesUtils.getInstance(confName).get("queueNamePrefix");
		int queueCount = PropertiesUtils.getInstance(confName).getInt("queueCount");
		String groupId = PropertiesUtils.getInstance(confName).get("servId");
		String userName = PropertiesUtils.getInstance(confName).get("userName");
		String userPwd = PropertiesUtils.getInstance(confName).get("userPwd");
		int qType = 1;

		IMQClient mqClient = new MQClientImpl();
		mqClient.setAuthInfo(userName, userPwd);
		for (int i = 0; i < queueCount; i++) {
			String queueName = String.format("%s%02d", queueNamePrefix, i);
			int resDec = mqClient.queueDeclare(queueName, false, false, true, groupId, qType);
			if (resDec != 0) {
				String err = String.format("%s create fail! error:%s", queueName, mqClient.GetLastErrorMessage());
				System.out.println(err);
			} else {
				String info = String.format("%s create success!", queueName);
				System.out.println(info);
			}
		}
		mqClient.close();

		try {
			Thread.sleep(3000L);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
