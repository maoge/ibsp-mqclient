 package test;

import ibsp.common.utils.CONSTS;
import ibsp.common.utils.PropertiesUtils;
import ibsp.mq.client.api.IMQClient;
import ibsp.mq.client.api.MQClientImpl;
import ibsp.mq.client.api.MQMessage;

public class SingleTopicPermnentProducer {

	public static void main(String[] args) {
		String confName = "test";
		String topic = PropertiesUtils.getInstance(confName).get("queueName");
		String userName = PropertiesUtils.getInstance(confName).get("userName");
		String userPwd = PropertiesUtils.getInstance(confName).get("userPwd");
		
		IMQClient mqClient = new MQClientImpl();
		mqClient.setAuthInfo(userName, userPwd);
		int retConn = mqClient.connect(topic);

		if (retConn == CONSTS.REVOKE_OK) {
			String info = String.format("Connect success.");
			System.out.println(info);
		} else {
			String err = String.format("Connect fail, error:%s.", mqClient.GetLastErrorMessage());
			System.out.println(err);
			return;
		}

		int totalCnt = 5000000;
		int packLen = 128;

		byte[] sendBuf = new byte[packLen];
		for (int i = 0; i < packLen; i++) {
			sendBuf[i] = (byte) (i % 128);
		}

		MQMessage message = new MQMessage();
		message.setBody(sendBuf);

		long start = System.currentTimeMillis();
		long lastTS = start;
		long currTS = start;

		long lastCnt = 0, currCnt = 0;

		while (currCnt < totalCnt) {
			long nanoTime = System.nanoTime();
			long miliTime = System.currentTimeMillis();
			String msgID = String.format("%s.%d", topic, nanoTime);
			message.setMessageID(msgID);
			message.setTimeStamp(miliTime);

			if (mqClient.publishTopic(topic, message) == CONSTS.REVOKE_OK) {
				currCnt++;

				if (currCnt % 200000 == 0) {
					currTS = System.currentTimeMillis();
					long diffTS = currTS - lastTS;
					long avgTPS = currCnt * 1000 / (currTS - start);
					long lastTPS = (currCnt - lastCnt) * 1000 / diffTS;

					String info = String.format("%s publish total: %d, lastTPS:%d, avgTPS:%d", topic, currCnt, lastTPS, avgTPS);
					System.out.println(info);

					lastTS = currTS;
					lastCnt = currCnt;
				}
			} else {
				String err = String.format("publish message to topic:%s fail, error message:%s", topic, mqClient.GetLastErrorMessage());
				System.err.println(err);
			}
			
//			try {
//				Thread.sleep(1);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
		}

		System.out.println("send complete!");
		mqClient.close();
	}

}
