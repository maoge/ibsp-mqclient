package test;

import com.ffcs.mq.client.api.IMQClient;
import com.ffcs.mq.client.api.MQClientImpl;
import com.ffcs.mq.client.api.MQMessage;
import com.ffcs.mq.client.utils.CONSTS;
import com.ffcs.mq.client.utils.PropertiesUtils;

public class SingleQueueProducer {
	
	private static int QUEUE_COUNT = 1;

	private static void testProducer(String queueNamePrefix, int packLen, int totalCnt) {
		
		IMQClient mqClient = new MQClientImpl();
		mqClient.setAuthInfo("admin", "admin");
		for (int i = 0; i < QUEUE_COUNT; i++) {
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
		}

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
			String queueName = String.format("%s%02d", queueNamePrefix, currCnt % QUEUE_COUNT);
			
			long nanoTime = System.nanoTime();
			long miliTime = System.currentTimeMillis();
			String msgID = String.format("%s.%d", queueName, nanoTime);
			message.setMessageID(msgID);
			message.setTimeStamp(miliTime);

			if (mqClient.sendQueue(queueName, message) == CONSTS.REVOKE_OK) {
				currCnt++;

				if (currCnt % 2000 == 0) {
					currTS = System.currentTimeMillis();
					long diffTS = currTS - lastTS;
					long avgTPS = currCnt * 1000 / (currTS - start);
					long lastTPS = (currCnt - lastCnt) * 1000 / diffTS;

					String info = String.format("%s send total: %d, lastTPS:%d, avgTPS:%d", queueName, currCnt, lastTPS, avgTPS);
					System.out.println(info);

					lastTS = currTS;
					lastCnt = currCnt;
				}
			} else {
				String err = String.format("Send message to queue:%s fail, error message:%s", queueName, mqClient.GetLastErrorMessage());
				System.err.println(err);
				
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		System.out.println("send complete, total count:" + currCnt);
		mqClient.close();
	}

	public static void main(String[] args) {
		String confName = "test";

		String queueNamePrefix = PropertiesUtils.getInstance(confName).get("queueNamePrefix");
		int packLen = PropertiesUtils.getInstance(confName).getInt("packLen");
		int totalCnt = 100000000;

		testProducer(queueNamePrefix, packLen, totalCnt);

	}

}
