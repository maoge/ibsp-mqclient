package ibsp.mq.client.router;

public class Broker {

	private String brokerId;
	private String brokerName;

	private String ip;
	private int port;
	private String mqUser;
	private String mqPwd;
	private String vhost;

	public Broker(String brokerId, String brokerName, String ip, int port, 
			String mqUser, String mqPwd, String vhost) {
		super();

		this.brokerId = brokerId;
		this.brokerName = brokerName;
		this.ip = ip;
		this.port = port;
		this.mqUser = mqUser;
		this.mqPwd = mqPwd;
		this.vhost = vhost;
	}

	public String getBrokerId() {
		return brokerId;
	}

	public void setBrokerId(String brokerId) {
		this.brokerId = brokerId;
	}

	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	public String getIP() {
		return ip;
	}

	public void setIP(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getMqUser() {
		return mqUser;
	}

	public void setMqUser(String mqUser) {
		this.mqUser = mqUser;
	}

	public String getMqPwd() {
		return mqPwd;
	}

	public void setMqPwd(String mqPwd) {
		this.mqPwd = mqPwd;
	}

	public String getVHost() {
		return vhost;
	}

	public void setVHost(String vhost) {
		this.vhost = vhost;
	}

	@Override
	public String toString() {
		return "Broker [brokerId=" + brokerId + ", brokerName=" + brokerName + ", ip=" + ip + 
				", port=" + port + ", mqUser=" + mqUser + ", mqPwd=" + mqPwd + ", vhost=" + vhost + "]";
	}

}
