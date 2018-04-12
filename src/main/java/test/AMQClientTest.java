package test;

import com.ffcs.mq.client.rabbit.RabbitMQNode;
import com.ffcs.mq.client.router.Broker;
import com.ffcs.mq.client.router.VBroker;
import com.ffcs.mq.client.utils.CONSTS;

public class AMQClientTest {

	public static void main(String[] args) {
		String userName = "mq";
		String passwd = "amqp";
		String vhost = "/";
		String host = "192.168.14.209";
		int port = 9230;

		Broker broker = new Broker("500", "b-500", "poc-206", host, host, port, 18999, userName, passwd, vhost, "UMPOMUHMYOTULGUVAWXI",
				true, "501", "vb-501", "1", "g-1");
		VBroker vbroker = new VBroker("vbrokerId", "vbrokerName", "500", host, 
				"UMPOMUHMYOTULGUVAWXI", false, true, "groupId", "groupName");
		vbroker.addBroker(broker);
		
		RabbitMQNode rabbitNode = new RabbitMQNode(vbroker);
		int connRes = rabbitNode.connect();
		System.out.println("connRes:" + connRes);

		if (connRes == CONSTS.REVOKE_OK) {
			//String queueName0 = "LBTEST_00";
			//int lsnRes0 = rabbitNode.listenQueue(queueName0);// , consumerTag);
			//System.out.println("lsnRes0:" + lsnRes0);
			
			// String queueName1 = "TT_01";
			// int lsnRes1 = rabbitNode.listenQueue(queueName1);
			// System.out.println("lsnRes1:" + lsnRes1);
			
			/*for (int i = 0; i < 100; i++) {
				String queueName = String.format("TT_%02d", i);
				int delResult = rabbitNode.queueDelete(queueName);
				String info = String.format("delete %s result:%s", queueName, delResult == 0 ? "success" : "fail");
				System.out.println(info);
			}*/
			
			for (int i = 0; i < 200; i++) {
				String queueName = String.format("TT_%02d", i);
				int decResult = rabbitNode.queueDeclare(queueName, true, false, false, null);
				String info = String.format("delete %s result:%s", queueName, decResult == 0 ? "success" : "fail");
				System.out.println(info);
			}
		}

		rabbitNode.close();

	}

}
