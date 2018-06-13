package test;

import java.util.ArrayList;

import ibsp.common.utils.CONSTS;
import ibsp.mq.client.api.MQMessage;
import ibsp.mq.client.rabbit.RabbitMQNode;
import ibsp.mq.client.router.Broker;
import ibsp.mq.client.router.VBroker;

public class DataTransfer {
	
	private static final int NO_DATA_MAX = 1000;
	private static final int BATCH_PRINT = 10000;

	public static void main(String[] args) {
//		String srcIP = "192.168.14.206";
//		int srcPort = 6801;
//		int srcMgrPort = 16801;
//		String srcUser = "mq";
//		String srcPwd = "amqp";
		
//		String desIP = "192.168.14.209";
//		int desPort = 6803;
//		int desMgrPort = 16803;
//		String desUser = "mq";
//		String desPwd = "amqp";
		
		String srcIP = "134.130.131.245";
		int srcPort = 9220;
		String srcUser = "mq";
		String srcPwd = "amqp";
		
		String desIP = "134.130.131.246";
		int desPort = 9230;
		String desUser = "mq";
		String desPwd = "amqp";
		
		Broker srcBroker = new Broker("500", "xx", srcIP, srcPort, srcUser, srcPwd, "/");
		VBroker srcVBroker = new VBroker("vbrokerId", "vbrokerName", "500", true);
		srcVBroker.addBroker(srcBroker);
		
		Broker desBroker = new Broker("501", "xx", desIP, desPort, desUser, desPwd, "/");
		VBroker desVBroker = new VBroker("vbrokerId", "vbrokerName", "501", true);
		desVBroker.addBroker(desBroker);
		
		ArrayList<String> queueList = new ArrayList<String>(24);
//		queueList.add("TT_00");
//		queueList.add("TT_01");
//		queueList.add("TT_02");
//		queueList.add("TT_03");
//		queueList.add("TT_04");
		
		queueList.add("PermQ_6lLvR3A5HE8yIkNF");
//		queueList.add("PermQ_511KV33glBdDxugX");
//		queueList.add("PermQ_5TH2vdXLzSkSwx15");
//		queueList.add("PermQ_6es9uxhUEiveuQAf");
//		queueList.add("PermQ_7T2JlyABuiG3b9mR");
//		queueList.add("PermQ_HzoaM7KF1Pf1fLRD");
//		queueList.add("PermQ_LdE1Cany8Obxzedc");
//		queueList.add("PermQ_MKCxxUAyVvNrshrK");
//		queueList.add("PermQ_MPR1teod2gFOFIwk");
//		queueList.add("PermQ_QQD2c5dsFEPEVuTq");
//		queueList.add("PermQ_SpDVDBPGE272qevL");
//		queueList.add("PermQ_YHUwdSe7cklwU3DE");
//		queueList.add("PermQ_b4tFH2qyHIw7EaV4");
//		queueList.add("PermQ_bo04ED3smKeoOjIl");
//		queueList.add("PermQ_dKsDQTBocH71QCre");
//		queueList.add("PermQ_e9ViEHantKVEpF9V");
//		queueList.add("PermQ_eDrVu7yO9KhMHvyQ");
//		queueList.add("PermQ_eXVLooIKQpMbbwxO");
//		queueList.add("PermQ_fp54eKUSC09OE7Wo");
//		queueList.add("PermQ_ftJvQe10hN12e7ny");
//		queueList.add("PermQ_nifJE7LPXwm6rfYq");
//		queueList.add("PermQ_xYuAGrBYnY3A4H5c");
		
		RabbitMQNode srcRabbitNode = new RabbitMQNode(srcVBroker);
		RabbitMQNode desRabbitNode = new RabbitMQNode(desVBroker);
		
		if (srcRabbitNode.connect() != CONSTS.REVOKE_OK) {
			System.out.println("source node connect fail!");
			return;
		}
		
		if (desRabbitNode.connect() != CONSTS.REVOKE_OK) {
			System.out.println("dest node connect fail!");
			return;
		}
		
		System.out.println("transfer begin ......");
		
		for (String queue : queueList) {
			if (srcRabbitNode.listenQueue(queue) != CONSTS.REVOKE_OK) {
				System.out.println("listen queue:" + queue + " fail!");
				break;
			}
			
			MQMessage msgRev = new MQMessage();
			MQMessage msgSend = new MQMessage();
			int noDataCounter = 0;
			int counter = 0;
			
			while (true) {
				int retConsume = srcRabbitNode.consumeMessage(queue, msgRev, 10);
				if (retConsume == 1) {
					noDataCounter = 0;
					
					msgSend.setBody(msgRev.getBody());
					msgSend.setMessageID(msgRev.getMessageID());
					msgSend.setTimeStamp(msgRev.getTimeStamp());
					msgSend.setSourceType(msgRev.getSourceType());
					msgSend.setSourceName(msgRev.getSourceName());
					
					if (desRabbitNode.sendQueue(queue, msgSend, true) != CONSTS.REVOKE_OK) {
						System.out.println("send message to queue:" + queue + " fail!");
					}
					
					srcRabbitNode.ackMessage(msgRev);
					
					if (++counter % BATCH_PRINT == 0) {
						System.out.println("queue:" + queue + " transfered count:" + counter);
					}
				} else if (retConsume == 0) {
					noDataCounter++;
					
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					
					if (noDataCounter > NO_DATA_MAX) {
						System.out.println("queue:" + queue + " transfer finish!");
						srcRabbitNode.unlistenQueue(queue);
						break;
					}
				}
			}
		}
		
		srcRabbitNode.close();
		desRabbitNode.close();
		
		System.out.println("transfer begin ......");
		
	}

}
