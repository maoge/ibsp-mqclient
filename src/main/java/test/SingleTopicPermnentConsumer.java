package test;

import ibsp.common.utils.CONSTS;
import ibsp.common.utils.PropertiesUtils;
import ibsp.mq.client.api.IMQClient;
import ibsp.mq.client.api.MQClientImpl;
import ibsp.mq.client.api.MQMessage;

public class SingleTopicPermnentConsumer {

	public static void main(String[] args) {
		String confName = "test";
		String topic = PropertiesUtils.getInstance(confName).get("queueName");
		String consumerId = PropertiesUtils.getInstance(confName).get("consumerId");
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

		if (mqClient.listenTopicPermnent(topic, consumerId) == CONSTS.REVOKE_OK) {
			String info = String.format("listen success.");
			System.out.println(info);
		} else {
			String err = String.format("listenTopicPermnent fail, error:%s.", mqClient.GetLastErrorMessage());
			System.out.println(err);

			mqClient.close();
			return;
		}

		int totalCnt = 5000000;
		MQMessage message = new MQMessage();

		long start = System.currentTimeMillis();
		long lastTS = start;
		long currTS = start;

		long lastCnt = 0, currCnt = 0;

		while (currCnt < totalCnt) {
			int retRecv = mqClient.consumeMessage(consumerId, message, 10);
			//int retRecv = mqClient.consumeMessage(message, 0);
			if (retRecv == 1) {
				if (mqClient.ackMessage(message) == CONSTS.REVOKE_OK) {
					currCnt++;

					if (currCnt % 200000 == 0) {
						currTS = System.currentTimeMillis();
						long diffTS = currTS - lastTS;
						long avgTPS = currCnt * 1000 / (currTS - start);
						long lastTPS = (currCnt - lastCnt) * 1000 / diffTS;

						String info = String.format("%s recv total: %d, lastTPS:%d, avgTPS:%d", topic, currCnt, lastTPS, avgTPS);
						System.out.println(info);

						lastTS = currTS;
						lastCnt = currCnt;
					}
				}
			} else if (retRecv < 0) {
				String err = String.format("consume error:%s", mqClient.GetLastErrorMessage());
				System.out.println(err);
			}
		}

		System.out.println("recv complete!");
		mqClient.unlistenTopicPermnent(consumerId);
		mqClient.close();
	}

}
