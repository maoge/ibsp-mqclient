package test;

import com.ffcs.mq.client.api.IMQClient;
import com.ffcs.mq.client.api.MQClientImpl;
import com.ffcs.mq.client.utils.PropertiesUtils;

public class DeleteQueueTest {

	public static void main(String[] args) {
		String confName = "test";
		String queueNamePrefix = PropertiesUtils.getInstance(confName).get("queueNamePrefix");
		int queueCount = PropertiesUtils.getInstance(confName).getInt("queueCount");

		IMQClient mqClient = new MQClientImpl();
		mqClient.setAuthInfo("admin", "admin");
		for (int i = 0; i < queueCount; i++) {
			String queueName = String.format("%s%02d", queueNamePrefix, i);
			int resDec = mqClient.queueDelete(queueName);
			if (resDec != 0) {
				String err = String.format("%s delete fail! error:%s", queueName, mqClient.GetLastErrorMessage());
				System.out.println(err);
			} else {
				String info = String.format("%s delete success!", queueName);
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
