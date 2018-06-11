package test;

import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import ibsp.mq.client.api.IMQClient;
import ibsp.mq.client.api.MQClientImpl;
import ibsp.mq.client.api.MQMessage;
import ibsp.mq.client.utils.CONSTS;
import ibsp.mq.client.utils.Global;
import ibsp.mq.client.utils.PropertiesUtils;

public class MultiTopicProducer {

	private static AtomicLong[] normalCntVec;
	private static AtomicLong[] errorCntVec;
	private static AtomicLong maxTPS;
	private static String userName;
	private static String userPwd;

	private static class TopicProducer implements Runnable {

		private String threadName;
		private String queueName;

		private int packLen;

		private AtomicLong normalCnt;
		private AtomicLong errorCnt;

		private boolean bRunning;

		public TopicProducer(String threadName, String queueName, int packLen, AtomicLong normalCnt, AtomicLong errorCnt) {
			this.threadName = threadName;
			this.queueName = queueName;
			this.packLen = packLen;
			this.normalCnt = normalCnt;
			this.errorCnt = errorCnt;
		}

		@Override
		public void run() {

			IMQClient mqClient = new MQClientImpl();
			mqClient.setAuthInfo(userName, userPwd);
			int retConn = mqClient.connect(queueName);

			if (retConn == CONSTS.REVOKE_OK) {
				bRunning = true;
				String info = String.format("%s connect success.", threadName);
				System.out.println(info);
			} else {
				bRunning = false;
				String err = String.format("%s connect %s fail, error:%s.", threadName, queueName, Global.get().getLastError());
				System.out.println(err);
			}

			byte[] sendBuf = new byte[packLen];
			for (int i = 0; i < packLen; i++) {
				sendBuf[i] = (byte) (i % 128);
			}

			MQMessage message = new MQMessage();
			message.setBody(sendBuf);

			long start = System.currentTimeMillis();

			while (bRunning) {

				long nanoTime = System.nanoTime();
				long miliTime = System.currentTimeMillis();
				String msgID = String.format("%s.%d", queueName, nanoTime);
				message.setMessageID(msgID);
				message.setTimeStamp(miliTime);

				if (mqClient.publishTopic(queueName, message) == CONSTS.REVOKE_OK) {
					long cnt = normalCnt.incrementAndGet();
					if (cnt % 20000 == 0) {
						String info = String.format("%s send message count:%d", threadName, cnt);
						System.out.println(info);
					}
				} else {
					errorCnt.incrementAndGet();
					String err = String
							.format("Send message to queue:%s fail, error message:%s", queueName, Global.get().getLastError());
					System.out.println(err);
				}
			}

			long totalSend = (long) (normalCnt.get());
			long end = System.currentTimeMillis();

			long timeSpend = end - start;
			System.out.println(threadName + " runs " + timeSpend / 1000 + " seconds, total send message count:" + totalSend
					+ ", average TPS:" + (totalSend * 1000) / timeSpend);
		}

		public void StopRunning() {
			bRunning = false;
		}
	}

	private static void testMultiProducer(String queueNamePrefix, int proCount, int packLen, int totalTime) {
		normalCntVec = new AtomicLong[proCount];
		errorCntVec = new AtomicLong[proCount];
		for (int i = 0; i < proCount; i++) {
			normalCntVec[i] = new AtomicLong(0);
			errorCntVec[i] = new AtomicLong(0);
		}
		maxTPS = new AtomicLong(0);

		Statistic stat = new Statistic(maxTPS, normalCntVec);
		Vector<TopicProducer> theadVec = new Vector<TopicProducer>(proCount);
		int idx = 0;

		long start = System.currentTimeMillis();
		long totalDiff = 0;

		for (; idx < proCount; idx++) {
			String threadName = String.format("TOPICPRODUCER_%d", idx);
			String queueName = String.format("%s%02d", queueNamePrefix, idx);

			TopicProducer topicProducer = new TopicProducer(threadName, queueName, packLen, normalCntVec[idx], errorCntVec[idx]);
			Thread thread = new Thread(topicProducer);
			thread.start();

			theadVec.add(topicProducer);
		}

		while (totalDiff < totalTime) {
			long curr = System.currentTimeMillis();
			totalDiff = (curr - start) / 1000;

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		for (TopicProducer pro : theadVec) {
			pro.StopRunning();
		}
		
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		stat.StopRunning();
	}

	public static void main(String[] args) {
		String confName = "test";

		String queueNamePrefix = PropertiesUtils.getInstance(confName).get("queueNamePrefix");
		int queueCount = PropertiesUtils.getInstance(confName).getInt("queueCount");
		int packLen = PropertiesUtils.getInstance(confName).getInt("packLen");
		int totalTime = PropertiesUtils.getInstance(confName).getInt("totalTime");
		userName = PropertiesUtils.getInstance(confName).get("userName");
		userPwd = PropertiesUtils.getInstance(confName).get("userPwd");

		testMultiProducer(queueNamePrefix, queueCount, packLen, totalTime);
	}

}
