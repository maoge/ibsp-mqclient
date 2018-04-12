package test;

import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import com.ffcs.mq.client.api.IMQClient;
import com.ffcs.mq.client.api.MQClientImpl;
import com.ffcs.mq.client.api.MQMessage;
import com.ffcs.mq.client.utils.CONSTS;
import com.ffcs.mq.client.utils.PropertiesUtils;

public class MultiQueueConsumer {

	private static AtomicLong[] normalCntVec;
	private static AtomicLong[] errorCntVec;
	private static AtomicLong maxTPS;

	private static class TopicConsumer implements Runnable {

		private String threadName;
		private String queueName;

		private AtomicLong normalCnt;
		private AtomicLong errorCnt;

		private boolean bRunning;

		public TopicConsumer(String threadName, String queueName, AtomicLong normalCnt, AtomicLong errorCnt) {
			this.threadName = threadName;
			this.queueName = queueName;
			this.normalCnt = normalCnt;
			this.errorCnt = errorCnt;
		}

		@Override
		public void run() {
			IMQClient mqClient = new MQClientImpl();
			mqClient.setAuthInfo("admin", "admin");
			int retConn = mqClient.connect(queueName);
			if (retConn == CONSTS.REVOKE_OK) {

				String info = String.format("%s connect success.", threadName);
				System.out.println(info);

				int retLsnr = mqClient.listenQueue(queueName);
				if (retLsnr == CONSTS.REVOKE_OK) {
					bRunning = true;

					String lsnrInfo = String.format("listenQueue:%s ok.", queueName);
					System.out.println(lsnrInfo);
				} else {
					String lsnrErr = String.format("listenQueue:%s fail.", queueName);
					System.out.println(lsnrErr);
				}
			} else {
				bRunning = false;
				String err = String.format("%s connect %s fail, error:%s.", threadName, queueName, mqClient.GetLastErrorMessage());
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
				int res = mqClient.consumeMessage(queueName, message, 10);
				if (res == 1) {
					if (mqClient.ackMessage(message) == CONSTS.REVOKE_OK) {
						long cnt = normalCnt.incrementAndGet();
						if (cnt % 200000 == 0) {
							String info = String.format("%s receive message count:%d", threadName, cnt);
							System.out.println(info);
						}
					}
				} else if (res < 0) {
					long errCnt = errorCnt.incrementAndGet();

					String err = String.format("consumeMessage err:%s", mqClient.GetLastErrorMessage());
					System.out.println(err);

					String sErrCnt = String.format("error count:%d", errCnt);
					System.out.println(sErrCnt);
				}
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

	private static void testMultiConsumer(String queueNamePrefix, int queueCount, int totalTime, int consumerCntPerQueue) {
		normalCntVec = new AtomicLong[queueCount * consumerCntPerQueue];
		errorCntVec = new AtomicLong[queueCount * consumerCntPerQueue];
		for (int i = 0; i < queueCount * consumerCntPerQueue; i++) {
			normalCntVec[i] = new AtomicLong(0);
			errorCntVec[i] = new AtomicLong(0);
		}
		maxTPS = new AtomicLong(0);

		Statistic stat = new Statistic(maxTPS, normalCntVec);
		Vector<TopicConsumer> theadVec = new Vector<TopicConsumer>(queueCount * consumerCntPerQueue);
		int idx = 0;

		long start = System.currentTimeMillis();
		long totalDiff = 0;

		for (; idx < queueCount; idx++) {
			String queueName = String.format("%s%02d", queueNamePrefix, idx);

			for (int conIdx = 0; conIdx < consumerCntPerQueue; conIdx++) {
				String threadName = String.format("QUEUECONSUMER_%02d_%02d", idx, conIdx);

				TopicConsumer topicProducer = new TopicConsumer(threadName, queueName, normalCntVec[idx], errorCntVec[idx]);
				Thread thread = new Thread(topicProducer);
				thread.start();

				theadVec.add(topicProducer);
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

		for (TopicConsumer con : theadVec) {
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

		String queueNamePrefix = PropertiesUtils.getInstance(confName).get("queueNamePrefix");
		int queueCount = PropertiesUtils.getInstance(confName).getInt("queueCount");
		int totalTime = PropertiesUtils.getInstance(confName).getInt("totalTime");
		int consumerCntPerQueue = PropertiesUtils.getInstance(confName).getInt("consumerCntPerQueue");

		testMultiConsumer(queueNamePrefix, queueCount, totalTime, consumerCntPerQueue);
	}

}
