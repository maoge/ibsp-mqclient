package test;

import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import ibsp.common.utils.CONSTS;
import ibsp.common.utils.PropertiesUtils;
import ibsp.mq.client.api.IMQClient;
import ibsp.mq.client.api.MQClientImpl;
import ibsp.mq.client.api.MQMessage;

public class MultiQueueProducer {

	private static AtomicLong[] normalCntVec;
	private static AtomicLong[] errorCntVec;
	private static AtomicLong maxTPS;
	private static String userName;
	private static String userPwd;

	private static class TopicProducer implements Runnable {

		private String threadName;
		private String queueName;

		private int packLen;
		private int priority;

		private AtomicLong normalCnt;
		private AtomicLong errorCnt;

		private boolean bRunning;

		public TopicProducer(String threadName, String queueName, int packLen, int priority, AtomicLong normalCnt, AtomicLong errorCnt) {
			this.threadName = threadName;
			this.queueName = queueName;
			this.packLen = packLen;
			this.priority = priority;
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
				String err = String.format("%s connect %s fail, error:%s.", threadName, queueName, mqClient.GetLastErrorMessage());
				System.out.println(err);
			}

			byte[] sendBuf = new byte[packLen];
			for (int i = 0; i < packLen; i++) {
				sendBuf[i] = (byte) (i % 128);
			}

			MQMessage message = new MQMessage();
			message.setBody(sendBuf);
			message.setPriority(priority);

			long start = System.currentTimeMillis();

			while (bRunning) {

				long nanoTime = System.nanoTime();
				long miliTime = System.currentTimeMillis();
				String msgID = String.format("%s.%d", queueName, nanoTime);
				message.setMessageID(msgID);
				message.setTimeStamp(miliTime);

				if (mqClient.sendQueue(queueName, message) == CONSTS.REVOKE_OK) {
					long cnt = normalCnt.incrementAndGet();
					if (cnt % 200000 == 0) {
						String info = String.format("%s send message count:%d", threadName, cnt);
						System.out.println(info);
					}
				} else {
					long errCnt = errorCnt.incrementAndGet();

					String err = String.format("%s, Send message to queue:%s fail, error message:%s, error count:%d",
							threadName, queueName, mqClient.GetLastErrorMessage(), errCnt);
					System.out.println(err);
				}
			}

			mqClient.close();

			long totalSend = (long) (normalCnt.get());
			long end = System.currentTimeMillis();

			long timeSpend = end - start;
			if (timeSpend > 0) {
				System.out.println(threadName + " runs " + timeSpend / 1000 + " seconds, total send message count:" + totalSend
						+ ", average TPS:" + (totalSend * 1000) / timeSpend);
			}

		}

		public void StopRunning() {
			bRunning = false;
		}
	}

	private static void testMultiProducer(String queueNamePrefix, int queueCount, int packLen, int totalTime, boolean priority) {
		normalCntVec = new AtomicLong[queueCount];
		errorCntVec = new AtomicLong[queueCount];
		for (int i = 0; i < queueCount; i++) {
			normalCntVec[i] = new AtomicLong(0);
			errorCntVec[i] = new AtomicLong(0);
		}
		maxTPS = new AtomicLong(0);

		Statistic stat = new Statistic(maxTPS, normalCntVec);
		Vector<TopicProducer> theadVec = new Vector<TopicProducer>(queueCount);
		int idx = 0;

		long start = System.currentTimeMillis();
		long totalDiff = 0;

		for (; idx < queueCount; idx++) {
			String threadName = String.format("QUEUE_PRODUCER_%02d", idx);
			String queueName = String.format("%s%02d", queueNamePrefix, idx);

			int nPriority = priority ? idx % CONSTS.MQ_MAX_QUEUE_PRIORITY : CONSTS.MQ_DEFAULT_QUEUE_PRIORITY;
			TopicProducer topicProducer = new TopicProducer(threadName, queueName, packLen, nPriority, normalCntVec[idx], errorCntVec[idx]);
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
		boolean priority = PropertiesUtils.getInstance(confName).getBoolean("priority", false);
		userName = PropertiesUtils.getInstance(confName).get("userName");
		userPwd = PropertiesUtils.getInstance(confName).get("userPwd");

		testMultiProducer(queueNamePrefix, queueCount, packLen, totalTime, priority);
	}

}
