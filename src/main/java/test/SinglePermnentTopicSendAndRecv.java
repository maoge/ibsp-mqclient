package test;

import com.ffcs.mq.client.api.IMQClient;
import com.ffcs.mq.client.api.MQClientImpl;
import com.ffcs.mq.client.api.MQMessage;
import com.ffcs.mq.client.utils.CONSTS;

public class SinglePermnentTopicSendAndRecv {

	public static void main(String[] args) {
		String topic1 = "abc.*";
		IMQClient mqClient = new MQClientImpl();
		mqClient.setAuthInfo("admin", "admin");
		int retConn = mqClient.connect(topic1);
		
		if (retConn == CONSTS.REVOKE_OK) {
			String info = String.format("Connect %s success.", topic1);
			System.out.println(info);
		} else {
			String err = String.format("Connect %s fail, error:%s.", topic1, mqClient.GetLastErrorMessage());
			System.out.println(err);
			return;
		}
		
		int packLen = 128;

		byte[] sendBuf = new byte[packLen];
		for (int i = 0; i < packLen; i++) {
			sendBuf[i] = (byte) (i % 128);
		}

		MQMessage message = new MQMessage();
		message.setBody(sendBuf);
		
		long nanoTime = System.nanoTime();
		long miliTime = System.currentTimeMillis();
		String msgID = String.format("%s.%d", topic1, nanoTime);
		message.setMessageID(msgID);
		message.setTimeStamp(miliTime);
		
		if (mqClient.publishTopic(topic1, message) == CONSTS.REVOKE_OK) {
			String s = String.format("send %s success", topic1);
			System.out.println(s);
		} else {
			String s = String.format("send %s fail", topic1);
			System.out.println(s);
		}
		
		String topic2 = "abc1.*";
		String consumerId = "ConID_XPYNEwos2TcRMFvf";
		retConn = mqClient.connect(topic2);
		if (retConn == CONSTS.REVOKE_OK) {
			String info = String.format("Connect %s success.", topic2);
			System.out.println(info);
		} else {
			String err = String.format("Connect %s fail, error:%s.", topic2, mqClient.GetLastErrorMessage());
			System.out.println(err);
			return;
		}
		
		if (mqClient.listenTopicPermnent(topic2, consumerId) == CONSTS.REVOKE_OK) {
			String s = String.format("listen %s %s success.", topic2, consumerId);
			System.out.println(s);
		} else {
			String s = String.format("listen %s %s fail, %s.", topic2, consumerId, mqClient.GetLastErrorMessage());
			System.out.println(s);
		}
	}

}
