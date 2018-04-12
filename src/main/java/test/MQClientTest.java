package test;

import com.ffcs.mq.client.api.IMQClient;
import com.ffcs.mq.client.api.MQClientImpl;
import com.ffcs.mq.client.utils.CONSTS;

public class MQClientTest {

	public static void main(String[] args) {
		IMQClient mqClient = new MQClientImpl();
		mqClient.setAuthInfo("admin", "admin");

//		String topicName = "QQTEST_00";
//		boolean durable = true;
//		String groupId = "50046";
//		int type = 1;
//		int retDeclare = mqClient.queueDeclare(topicName, durable, groupId, type);
//		System.out.println("retDeclare:" + retDeclare);
		
//		int delRet = mqClient.queueDelete(topicName);
//		System.out.println("delRet:" + delRet);

//		int retConn = mqClient.connect(topicName);
//		System.out.println("retConn:" + retConn);

		// String consumerId = mqClient.genConsumerId();
		// int retLsnr = mqClient.listenTopicPermernent(topicName, consumerId);
		// System.out.println("retLsnr:" + retLsnr + ", consumerId:" +
		// consumerId);

//		String consumerId = "ConID_SaOAqPa57MBwwr0T";
//		int retDel = mqClient.logicQueueDelete(consumerId);
//		System.out.println("retDel:" + retDel);
		
		for (int i = 0; i < 3; i++) {
			String queueName = String.format("AAA_%02d", i);
			int decResult = mqClient.queueDeclare(queueName, false, true, "50147", 1);
			
			String info = decResult == CONSTS.REVOKE_OK ? String.format("declare queue:%s %s", queueName, (decResult == 0) ? "true" : "false") :
					String.format("declare queue:%s %s, error:%s", queueName, (decResult == 0) ? "true" : "false", mqClient.GetLastErrorMessage());
			System.out.println(info);
		}
		
//		for (int i = 220; i < 390; i++) {
//			String queueName = String.format("TT_%03d", i);
//			int delResult = mqClient.queueDelete(queueName);
//			
//			String info = String.format("delResult queue:%s %s", queueName, (delResult == 0) ? "true" : "false");
//			System.out.println(info);
//		}
		
//		int retPurge = mqClient.purgeQueue("TT_00");
//		System.out.println("purge result:" + retPurge);
		
//		int retPurge = mqClient.purgeTopic("ConID_zE0QbAmt1jBsdCM1");
//		System.out.println("purge result:" + retPurge);
		
		mqClient.close();
		
	}

}
