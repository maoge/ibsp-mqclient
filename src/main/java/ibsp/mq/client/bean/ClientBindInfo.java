package ibsp.mq.client.bean;

import java.util.ArrayList;
import java.util.List;

public class ClientBindInfo {
	
	private String clientID;
	private ArrayList<LiteQueueBean> listenList;   // all listened queue or topic
	private int rbQueueSeed = 0;
	private int size = 0;
	
	public ClientBindInfo(String clientID) {
		this.clientID = clientID;
		this.listenList = new ArrayList<LiteQueueBean>();
	}

	public String getClientID() {
		return clientID;
	}

	public void setClientID(String clientID) {
		this.clientID = clientID;
	}
	
	public String getNextConsumeQueue() {
		if (++rbQueueSeed < 0)
			rbQueueSeed = 0;
		
		int idx = rbQueueSeed % size;
		LiteQueueBean bean = listenList.get(idx);
		return bean.getName();
	}
	

	
	public void addListen(LiteQueueBean bean) {
		if (listenList.contains(bean))
			return;
		
		listenList.add(bean);
		size++;
	}
	
	public void unlisten(String name) {
		for (LiteQueueBean bean : listenList) {
			if (name.equals(bean.getName())) {
				listenList.remove(bean);
				size--;
				break;
			}
		}
	}
	
	public List<LiteQueueBean> getListenList() {
		return this.listenList;
	}
	
	public boolean isListened(String name) {
		boolean ret = false;
		for (LiteQueueBean bean : listenList) {
			if (name.equals(bean.getName())) {
				ret = true;
				break;
			}
		}
		return ret;
	}
	
	public int listenSize() {
		return size;
	}
	
}
