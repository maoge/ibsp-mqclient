package test;

import com.ffcs.mq.client.utils.BasicOperation;
import com.ffcs.mq.client.utils.Global;
import com.ffcs.mq.client.utils.SVarObject;

public class HttpTest {

	public static void main(String[] args) {
		//String url = "http://192.168.37.175:9991/configsvr/getQueueByName?qname=TT_00";
		//String url = "http://192.168.14.205:9991/configsvr/getQueueByName?qname=TT_00";

		long beg = System.currentTimeMillis();
		int totalCnt = 1;
		//for (int i = 0; i < totalCnt; i++) {
			SVarObject sVar = new SVarObject();
			//boolean ret = HttpUtils.getData(url, sVar);
			//if (ret)
			//	System.out.println(sVar.getVal());
			//boolean bAuth = BasicOperation.auth("admin", "admin");
			
			//String info = String.format("auth return:%s, magicKey:%s", bAuth ? "true" : "false", Global.get().getMagicKey());
			//System.out.println(info);
			
			
			//BasicOperation.loadQueueByName("TT_00", sVar);
			//BasicOperation.loadQueueBrokerRealtion("TT_00", sVar);
			//BasicOperation.queueDeclare("sdasda", true, "50055", "1", sVar);
			//BasicOperation.queueDelete("sdasda", sVar);
			//BasicOperation.getLocalIP(sVar);
			//BasicOperation.genConsumerID(sVar);
			//BasicOperation.genPermQueue(sVar);
			
			//SVarObject sVarSrcQueue = new SVarObject();
			//SVarObject sVarRealQueue = new SVarObject();
			//SVarObject sVarMainKey = new SVarObject();
			//SVarObject sVarSubKey = new SVarObject();
			//SVarObject sVarGroupId = new SVarObject();
			
			//BasicOperation.getPermnentTopic("ConID_JoWa08BuvAEaxAze", sVarSrcQueue, sVarRealQueue, sVarMainKey,
			//		sVarSubKey, sVarGroupId);
			//System.out.println(sVarSrcQueue.getVal());
			//System.out.println(sVarRealQueue.getVal());
			//System.out.println(sVarMainKey.getVal());
			//System.out.println(sVarSubKey.getVal());
			//System.out.println(sVarGroupId.getVal());
			
			int cnt = BasicOperation.getMessageReady("TT_00");
			
			System.out.println(cnt);
			
		//}
		long end = System.currentTimeMillis();
		long ts = end - beg;
		
		
		//long tps = (totalCnt * 1000) / ts;
		
		//String info = String.format("ts:%d, tps:%d", ts, tps);
		//System.out.println(info);
		
		Global.get().shutdown();
		
		return;
	}

}
