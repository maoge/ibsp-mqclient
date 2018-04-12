package test;

import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import com.ffcs.mq.client.api.IMQClient;
import com.ffcs.mq.client.api.MQClientImpl;
import com.ffcs.mq.client.api.MQMessage;
import com.ffcs.mq.client.utils.CONSTS;
import com.ffcs.mq.client.utils.PropertiesUtils;

public class MultiWildcardTopicConsumer {

	private static AtomicLong[] normalCntVec;
	private static AtomicLong[] errorCntVec;
	private static AtomicLong maxTPS;

	private static class WildcardTopicConsumer implements Runnable {

		private String threadName;
		private String mainKey;
		private String subKey;
		private String consumerID;

		private AtomicLong normalCnt;
		private AtomicLong errorCnt;

		private boolean bRunning;

		public WildcardTopicConsumer(String threadName, String mainKey, String subKey, AtomicLong normalCnt, AtomicLong errorCnt) {
			this.threadName = threadName;
			this.mainKey    = mainKey;
			this.subKey     = subKey;
			this.normalCnt  = normalCnt;
			this.errorCnt   = errorCnt;
		}

		@Override
		public void run() {
			boolean needDeleteConsumerID = false;

			IMQClient mqClient = new MQClientImpl();
			mqClient.setAuthInfo("admin", "admin");
			int retConn = mqClient.connect(mainKey);
			if (retConn == CONSTS.REVOKE_OK) {
				String info = String.format("%s connect success.", threadName);
				System.out.println(info);

				consumerID = mqClient.genConsumerId();
				int retLsnr = mqClient.listenTopicWildcard(mainKey, subKey, consumerID);
				if (retLsnr == CONSTS.REVOKE_OK) {
					bRunning = true;
					needDeleteConsumerID = true;

					String lsnrInfo = String.format("listenTopicWildcard:%s %s ok, consumerID:%s", mainKey, subKey, consumerID);
					System.out.println(lsnrInfo);
				} else {
					String lsnrErr = String.format("listenTopicWildcard:%s %s fail.", mainKey, subKey);
					System.out.println(lsnrErr);
				}
			} else {
				bRunning = false;
				String err = String.format("%s connect %s fail, error:%s.", threadName, mainKey, mqClient.GetLastErrorMessage());
				System.out.println(err);
			}
			
			// 大量连接时偶尔java.net.SocketException: Broken pipe
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			MQMessage message = new MQMessage();

			long start = System.currentTimeMillis();
			while (bRunning) {
				int res = mqClient.consumeMessageWildcard(mainKey, subKey, consumerID, message, 0);
				if (res == 1) {
					if (mqClient.ackMessage(message) == CONSTS.REVOKE_OK) {
						long cnt = normalCnt.incrementAndGet();
						if (cnt % 20000 == 0) {
							String info = String.format("%s receive message count:%d", threadName, cnt);
							System.out.println(info);
						}
					}
				} else if (res < 0) {
					errorCnt.incrementAndGet();

					String err = String.format("consumeMessage err:%s", mqClient.GetLastErrorMessage());
					System.out.println(err);
				}

			}

			if (needDeleteConsumerID) {
				mqClient.unlistenTopicWildcard(consumerID);
				mqClient.logicQueueDelete(consumerID);
			}
			
			mqClient.close();

			long totalSend = (long) (normalCnt.get());
			long end = System.currentTimeMillis();

			long timeSpend = end - start;
			if (timeSpend > 0) {
				System.out.println(threadName + " runs " + timeSpend / 1000 + " seconds, total received message count:" + totalSend
						+ ", average TPS:" + (totalSend * 1000) / timeSpend);
			}
		}

		public void StopRunning() {
			bRunning = false;
		}

	}

	private static void testMultiConsumer(int proCount, int packLen, int totalTime, int consumerCntPerQueue) {
		normalCntVec = new AtomicLong[proCount * consumerCntPerQueue];
		errorCntVec = new AtomicLong[proCount * consumerCntPerQueue];
		for (int i = 0; i < proCount * consumerCntPerQueue; i++) {
			normalCntVec[i] = new AtomicLong(0);
			errorCntVec[i] = new AtomicLong(0);
		}
		maxTPS = new AtomicLong(0);

		Statistic stat = new Statistic(maxTPS, normalCntVec);
		Vector<WildcardTopicConsumer> theadVec = new Vector<WildcardTopicConsumer>(proCount);
		int idx = 0;

		long start = System.currentTimeMillis();
		long totalDiff = 0;

		for (; idx < proCount; idx++) {
			String mainKey = "abc.*";
			String subKey = String.format("%s.%02d", "abc", idx);

			for (int conIdx = 0; conIdx < consumerCntPerQueue; conIdx++) {
				String threadName = String.format("WILDCARD_TOPIC_CONSUMER_%02d_%02d", idx, conIdx);

				WildcardTopicConsumer topicConsumer = new WildcardTopicConsumer(threadName, mainKey, subKey, normalCntVec[idx * consumerCntPerQueue
						+ conIdx], errorCntVec[idx * consumerCntPerQueue + conIdx]);
				Thread thread = new Thread(topicConsumer);
				thread.start();

				theadVec.add(topicConsumer);
			}
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

		for (WildcardTopicConsumer con : theadVec) {
			con.StopRunning();
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

		int proCount = PropertiesUtils.getInstance(confName).getInt("queueCount");
		int packLen = PropertiesUtils.getInstance(confName).getInt("packLen");
		int totalTime = PropertiesUtils.getInstance(confName).getInt("totalTime");
		int consumerCntPerQueue = PropertiesUtils.getInstance(confName).getInt("consumerCntPerQueue");

		testMultiConsumer(proCount, packLen, totalTime, consumerCntPerQueue);
	}

}
