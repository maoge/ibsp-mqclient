package test;

import ibsp.mq.client.api.IMQClient;
import ibsp.mq.client.api.MQClientImpl;
import ibsp.mq.client.api.MQMessage;
import ibsp.mq.client.utils.CONSTS;
import ibsp.mq.client.utils.PropertiesUtils;

public class SingleClientListenMultiQueue {
	
	private static String userName;
	private static String userPwd;
	
	private static void testConsumer(String queueNamePrefix, int queueCnt) {
		if (queueCnt <= 0)
			return;
		
		IMQClient mqClient = new MQClientImpl();
		mqClient.setAuthInfo(userName, userPwd);
		for (int i = 0; i < queueCnt; i++) {
			String queueName = String.format("%s%02d", queueNamePrefix, i);
			
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
		}
		
		MQMessage message = new MQMessage();
		long start = System.currentTimeMillis();
		long lastTS = start;
		long currTS = start;
		long lastCnt = 0, currCnt = 0, errorCnt = 0;
		boolean bRunning = true;
		while (bRunning) {
			int res = mqClient.consumeMessage(message, 0);
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
		
		mqClient.close();
	}

	public static void main(String[] args) {
		String confName = "test";
		String queueNamePrefix = PropertiesUtils.getInstance(confName).get("queueNamePrefix");
		int queueCount = PropertiesUtils.getInstance(confName).getInt("queueCount");
		userName = PropertiesUtils.getInstance(confName).get("userName");
		userPwd = PropertiesUtils.getInstance(confName).get("userPwd");
		
		testConsumer(queueNamePrefix, queueCount);
	}

}
