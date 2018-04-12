package test;

import com.ffcs.mq.client.api.IMQClient;
import com.ffcs.mq.client.api.MQClientImpl;

public class BatchCreateQueueTest {

	public static void main(String[] args) {
		IMQClient mqClient = new MQClientImpl();
		
		for (int i = 0; i < 100; i++) {
			String queueName = String.format("TT_%02d", i);
			int retDeclare = mqClient.queueDeclare(queueName, true, "0", 1);
			System.out.println("queueName:" + queueName + " create " + (retDeclare == 0 ? "success!" : "fail!"));
			
			//int retDelete = mqClient.queueDelete(queueName);
			//System.out.println("queueName:" + queueName + " delete " + (retDelete == 0 ? "success!" : "fail!"));
			
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		mqClient.close();
	}

}
