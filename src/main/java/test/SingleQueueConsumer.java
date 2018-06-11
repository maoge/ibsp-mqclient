package test;

import ibsp.mq.client.api.IMQClient;
import ibsp.mq.client.api.MQClientImpl;
import ibsp.mq.client.api.MQMessage;
import ibsp.mq.client.utils.CONSTS;
import ibsp.mq.client.utils.PropertiesUtils;

public class SingleQueueConsumer {
	
	private static String userName;
	private static String userPwd;
	
	private static void testConsumer(String queueName, int packLen, int totalCnt) {
		IMQClient mqClient = new MQClientImpl();
		mqClient.setAuthInfo(userName, userPwd);
		int retConn = mqClient.connect(queueName);
		if (retConn == CONSTS.REVOKE_OK) {
			String info = String.format("Connect success.");
			System.out.println(info);
		} else {
			String err = String.format("Connect fail, error:%s.", mqClient.GetLastErrorMessage());
			System.out.println(err);
			mqClient.close();
			return;
		}
		
		if (mqClient.listenQueue(queueName) == CONSTS.REVOKE_OK) {
			String info = String.format("listen queue:%s success.", queueName);
			System.out.println(info);
		} else {
			String err = String.format("listen queue:%s fail, error:%s.", queueName, mqClient.GetLastErrorMessage());
			System.out.println(err);
			mqClient.close();
			return;
		}
		
		MQMessage message = new MQMessage();
		
		long start = System.currentTimeMillis();
		long lastTS = start;
		long currTS = start;
		long lastCnt = 0, currCnt = 0, errorCnt = 0;
		
		while (currCnt < totalCnt) {
			int res = mqClient.consumeMessage(queueName, message, 10);
			if (res == 1) {
				if (mqClient.ackMessage(message) == CONSTS.REVOKE_OK) {
					if (++currCnt % 2000 == 0) {
						currTS = System.currentTimeMillis();
						long diffTS = currTS - lastTS;
						long avgTPS = currCnt * 1000 / (currTS - start);
						long lastTPS = (currCnt - lastCnt) * 1000 / diffTS;
						
						String info = String.format("receive message count:%d, TPS:%d, avgTPS:%d", currCnt, lastTPS, avgTPS);
						System.out.println(info);
						
						lastTS = currTS;
						lastCnt = currCnt;
					}
				}
			} else if (res < 0) {
				String err = String.format("total errorCnt:%d, consumeMessage err:%s", ++errorCnt, mqClient.GetLastErrorMessage());
				System.out.println(err);
			}
		}
		
		System.out.println("receive complete, total count:" + currCnt);
		mqClient.close();
	}

	public static void main(String[] args) {
		String confName = "test";

		String queueName = PropertiesUtils.getInstance(confName).get("queueName");
		int packLen = PropertiesUtils.getInstance(confName).getInt("packLen");
		int totalCnt = 100000000;
		userName = PropertiesUtils.getInstance(confName).get("userName");
		userPwd = PropertiesUtils.getInstance(confName).get("userPwd");

		testConsumer(queueName, packLen, totalCnt);
	}

}
