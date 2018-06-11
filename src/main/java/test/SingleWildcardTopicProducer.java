package test;

import ibsp.mq.client.api.IMQClient;
import ibsp.mq.client.api.MQClientImpl;
import ibsp.mq.client.api.MQMessage;
import ibsp.mq.client.utils.CONSTS;
import ibsp.mq.client.utils.PropertiesUtils;

public class SingleWildcardTopicProducer {

	public static void main(String[] args) {
		String confName = "test";
		String mainKey = PropertiesUtils.getInstance(confName).get("queueName");
		String subKey = PropertiesUtils.getInstance(confName).get("subKey");
		String userName = PropertiesUtils.getInstance(confName).get("userName");
		String userPwd = PropertiesUtils.getInstance(confName).get("userPwd");

		IMQClient mqClient = new MQClientImpl();
		mqClient.setAuthInfo(userName, userPwd);
		int retConn = mqClient.connect(mainKey);

		if (retConn == CONSTS.REVOKE_OK) {
			String info = String.format("Connect success.");
			System.out.println(info);
		} else {
			String err = String.format("Connect fail, error:%s.", mqClient.GetLastErrorMessage());
			System.out.println(err);
			return;
		}

		int totalCnt = 10000000;
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
			String msgID = String.format("%s.%d", subKey, nanoTime);
			message.setMessageID(msgID);
			message.setTimeStamp(miliTime);

			if (mqClient.publishTopicWildcard(mainKey, subKey, message) == CONSTS.REVOKE_OK) {
				currCnt++;

				if (currCnt % 20000 == 0) {
					currTS = System.currentTimeMillis();
					long diffTS = currTS - lastTS;
					long avgTPS = currCnt * 1000 / (currTS - start);
					long lastTPS = (currCnt - lastCnt) * 1000 / diffTS;

					String info = String.format("%s publish total: %d, lastTPS:%d, avgTPS:%d", subKey, currCnt, lastTPS, avgTPS);
					System.out.println(info);

					lastTS = currTS;
					lastCnt = currCnt;
				}
			} else {
				String err = String.format("publish message to topic:%s fail, error message:%s", subKey, mqClient.GetLastErrorMessage());
				System.err.println(err);
			}
		}

		System.out.println("send complete!");
		mqClient.close();
	}

}
