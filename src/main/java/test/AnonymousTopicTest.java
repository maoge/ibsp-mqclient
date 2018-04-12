package test;

import com.ffcs.mq.client.rabbit.RabbitMQNode;
import com.ffcs.mq.client.router.Broker;
import com.ffcs.mq.client.router.VBroker;
import com.ffcs.mq.client.utils.CONSTS;

public class AnonymousTopicTest {

	public static void main(String[] args) {
		String userName = "mq";
		String passwd = "amqp";
		String vhost = "/";
		String host = "192.168.14.206";
		int port = 8999;

		String topic = "TT_00";

		Broker broker = new Broker("500", "b-500", "poc-206", host, host, port, 18999, userName, passwd, vhost, "UMPOMUHMYOTULGUVAWXI",
				true, "501", "vb-501", "1", "g-1");
		VBroker vbroker = new VBroker("vbrokerId", "vbrokerName", "500", host, 
				"UMPOMUHMYOTULGUVAWXI", false, true, "groupId", "groupName");
		vbroker.addBroker(broker);
		
		RabbitMQNode rabbitNode = new RabbitMQNode(vbroker);
		int connRes = rabbitNode.connect();
		System.out.println("connRes:" + connRes);

		if (connRes != CONSTS.REVOKE_OK) {
			return;
		}

		int decRes = rabbitNode.listenTopicAnonymous(topic);
		System.out.println("decRes:" + decRes);

		try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		rabbitNode.close();

	}

}
