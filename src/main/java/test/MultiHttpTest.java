package test;

import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import com.ffcs.mq.client.utils.BasicOperation;
import com.ffcs.mq.client.utils.Global;
import com.ffcs.mq.client.utils.PropertiesUtils;
import com.ffcs.mq.client.utils.SVarObject;

public class MultiHttpTest {
	
	private static AtomicLong[] normalCntVec;
	private static AtomicLong[] errorCntVec;
	private static AtomicLong maxTPS;
	
	private static class HttpRunner implements Runnable {
		
		private String threadName;
		private volatile boolean bRunning;
		
		private AtomicLong normalCnt;
		private AtomicLong errorCnt;
		
		public HttpRunner(String threadName, AtomicLong normalCnt, AtomicLong errorCnt) {
			this.threadName = threadName;
			this.normalCnt = normalCnt;
			this.errorCnt = errorCnt;
		}
		
		
		@Override
		public void run() {
			bRunning = true;
			long i = 0;
			SVarObject sVar = new SVarObject();
			
			while (bRunning) {
				String queueName = String.format("TT_%02d", i++ % 40); 
				
				sVar.clear();
				boolean ret = BasicOperation.loadQueueByName(queueName, sVar);
				if (ret) {
					long cnt = normalCnt.incrementAndGet();
					
					if (cnt % 10000 == 0) {
						String info = String.format("%s send request count:%d", threadName, cnt);
						System.out.println(info);
					}
				} else {
					long errCnt = errorCnt.incrementAndGet();
					String err = String.format("http request, error count:%d, error info:%s", errCnt, sVar.getVal());
					System.out.println(err);
				}
			}
			
		}
		
		public void StopRunning() {
			bRunning = false;
		}
		
	}

	public static void main(String[] args) {
		String confName = "test";
		int threadCnt = PropertiesUtils.getInstance(confName).getInt("queueCount");
		int totalTime = PropertiesUtils.getInstance(confName).getInt("totalTime");
		
		normalCntVec = new AtomicLong[threadCnt];
		errorCntVec = new AtomicLong[threadCnt];
		for (int i = 0; i < threadCnt; i++) {
			normalCntVec[i] = new AtomicLong(0);
			errorCntVec[i] = new AtomicLong(0);
		}
		maxTPS = new AtomicLong(0);
		
		Statistic stat = new Statistic(maxTPS, normalCntVec);
		Vector<HttpRunner> theadVec = new Vector<HttpRunner>(threadCnt);
		
		long start = System.currentTimeMillis();
		long totalDiff = 0;
		
		for (int idx = 0; idx < threadCnt; idx++) {
			String threadName = String.format("HTTPRUNNER_%02d", idx);
			HttpRunner runner = new HttpRunner(threadName, normalCntVec[idx], errorCntVec[idx]);
			Thread thread = new Thread(runner);
			thread.start();
			
			theadVec.add(runner);
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
		
		for (HttpRunner runner : theadVec) {
			runner.StopRunning();
		}

		stat.StopRunning();
		
		Global.get().shutdown();
		
	}

}