package test;

import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import ibsp.mq.client.api.IMQClient;
import ibsp.mq.client.api.MQClientImpl;
import ibsp.mq.client.api.MQMessage;
import ibsp.mq.client.utils.CONSTS;

public class PlcaConsumerMain {
	
	private static final String TRI_FORMAT = "c_plca_usage_notify_msg_%d_tri";
	private static final String RELA_FORMAT = "c_plca_usage_notify_msg_%d_rela";
	private static final String TOPIC_FORMAT = "plca_usage_notify_msg_%d";
	
	private static AtomicLong[] normalCntVec;
	private static AtomicLong[] errorCntVec;
	private static AtomicLong maxTPS;
	
	private static class PlcaConsumer implements Runnable {
		
		private String triConIDName;
		private String relaConIDName;
		private String topicName;
		
		private AtomicLong normalCnt;
		private AtomicLong errorCnt;
		
		private volatile boolean bRunning = false;
		
		public PlcaConsumer(int idx, AtomicLong normalCnt, AtomicLong errorCnt) {
			topicName = String.format(TOPIC_FORMAT, idx);
			triConIDName = String.format(TRI_FORMAT, idx);
			relaConIDName = String.format(RELA_FORMAT, idx);
			
			this.normalCnt = normalCnt;
			this.errorCnt = errorCnt;
		}

		@Override
		public void run() {
			IMQClient mqClient = new MQClientImpl();
			mqClient.setAuthInfo("admin", "admin_1705");
			
			int retConn = mqClient.connect(topicName);
			if (retConn == CONSTS.REVOKE_OK) {
				String info = String.format("connect %s success.", topicName);
				System.out.println(info);
				
				int retLsnr = mqClient.listenTopicPermnent(topicName, triConIDName);
				if (retLsnr == CONSTS.REVOKE_OK) {
					String lsnrInfo = String.format("listenTopicPermernent:%s ok, consumerID:%s", topicName, triConIDName);
					System.out.println(lsnrInfo);
				} else {
					String lsnrErr = String.format("listenTopicPermernent:%s consumerID:%s fail.", topicName, triConIDName);
					System.out.println(lsnrErr);
					return;
				}
				
				retLsnr = mqClient.listenTopicPermnent(topicName, relaConIDName);
				if (retLsnr == CONSTS.REVOKE_OK) {
					bRunning = true;
					String lsnrInfo = String.format("listenTopicPermernent:%s ok, consumerID:%s", topicName, relaConIDName);
					System.out.println(lsnrInfo);
				} else {
					String lsnrErr = String.format("listenTopicPermernent:%s consumerID:%s fail.", topicName, relaConIDName);
					System.out.println(lsnrErr);
					return;
				}
				
			} else {
				String err = String.format("connect %s fail, error:%s.", topicName, mqClient.GetLastErrorMessage());
				System.out.println(err);
				return;
			}
			
			try {
				Thread.sleep(3);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			MQMessage message = new MQMessage();
			long start = System.currentTimeMillis();
			while (bRunning) {
				int res = mqClient.consumeMessage(triConIDName, message, 0);
				if (res == 1) {
					if (mqClient.ackMessage(message) == CONSTS.REVOKE_OK) {
						long cnt = normalCnt.incrementAndGet();
						if (cnt % 20000 == 0) {
							String info = String.format("%s receive message count:%d", topicName, cnt);
							System.out.println(info);
						}
					}
				} else if (res < 0) {
					errorCnt.incrementAndGet();

					String err = String.format("consumeMessage err:%s", mqClient.GetLastErrorMessage());
					System.out.println(err);
				}
				
				res = mqClient.consumeMessage(relaConIDName, message, 0);
				if (res == 1) {
					if (mqClient.ackMessage(message) == CONSTS.REVOKE_OK) {
						long cnt = normalCnt.incrementAndGet();
						if (cnt % 20000 == 0) {
							String info = String.format("%s receive message count:%d", topicName, cnt);
							System.out.println(info);
						}
					}
				} else if (res < 0) {
					errorCnt.incrementAndGet();

					String err = String.format("consumeMessage err:%s", mqClient.GetLastErrorMessage());
					System.out.println(err);
				}
			}
			
			mqClient.unlistenTopicPermnent(triConIDName);
			mqClient.unlistenTopicPermnent(relaConIDName);
			mqClient.close();
			
			long totalRev = (long) (normalCnt.get());
			long end = System.currentTimeMillis();

			long timeSpend = end - start;
			if (timeSpend > 0) {
				System.out.println(topicName + " runs " + timeSpend / 1000 + " seconds, total received message count:" + totalRev
						+ ", average TPS:" + (totalRev * 1000) / timeSpend);
			}
		}
		
		public void StopRunning() {
			bRunning = false;
		}
		
	}

	
	private static void start(int cnt, int totalTimeSec) {
		normalCntVec = new AtomicLong[cnt];
		errorCntVec = new AtomicLong[cnt];
		for (int i = 0; i < cnt; i++) {
			normalCntVec[i] = new AtomicLong(0);
			errorCntVec[i] = new AtomicLong(0);
		}
		maxTPS = new AtomicLong(0);
		
		Statistic stat = new Statistic(maxTPS, normalCntVec);
		Vector<PlcaConsumer> theadVec = new Vector<PlcaConsumer>(cnt);
		int idx = 1;
		
		long start = System.currentTimeMillis();
		long totalDiff = 0;
		
		for (; idx <= cnt; idx++) {
			PlcaConsumer consumer = new PlcaConsumer(idx, normalCntVec[idx-1], errorCntVec[idx-1]);
			Thread thread = new Thread(consumer);
			thread.start();
			
			theadVec.add(consumer);
		}
		
		while (totalDiff < totalTimeSec) {
			long curr = System.currentTimeMillis();
			totalDiff = (curr - start) / 1000;

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		for (PlcaConsumer con : theadVec) {
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
		int cnt = 16;
		int totalTimeSec = 12*3600;
		
		start(cnt, totalTimeSec);
	}

}
