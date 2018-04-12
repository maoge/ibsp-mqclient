package com.ffcs.mq.client.router;

public class Broker {

	private String brokerId;
	private String brokerName;

	private String hostName;
	private String ip;
	private String vip;
	private int port;
	private int mgrPort;
	private String mqUser;
	private String mqPwd;
	private String vhost;

	private String erlCookie;
	private boolean isCluster;
	private String vbrokerId;
	private String vbrokerName;
	private String groupId;
	private String groupName;

	public Broker(String brokerId, String brokerName, String hostName, String ip, String vip, int port, int mgrPort, String mqUser,
			String mqPwd, String vhost, String erlCookie, boolean isCluster, String vbrokerId, String vbrokerName, String groupId,
			String groupName) {
		super();

		this.brokerId = brokerId;
		this.brokerName = brokerName;
		this.hostName = hostName;
		this.ip = ip;
		this.vip = vip;
		this.port = port;
		this.mgrPort = mgrPort;
		this.mqUser = mqUser;
		this.mqPwd = mqPwd;
		this.vhost = vhost;
		this.erlCookie = erlCookie;
		this.isCluster = isCluster;
		this.vbrokerId = vbrokerId;
		this.vbrokerName = vbrokerName;
		this.groupId = groupId;
		this.groupName = groupName;
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

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getIP() {
		return ip;
	}

	public void setIP(String ip) {
		this.ip = ip;
	}

	public String getVip() {
		return vip;
	}

	public void setVip(String vip) {
		this.vip = vip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getMgrPort() {
		return mgrPort;
	}

	public void setMgrPort(int mgrPort) {
		this.mgrPort = mgrPort;
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

	public String getErlCookie() {
		return erlCookie;
	}

	public void setErlCookie(String erlCookie) {
		this.erlCookie = erlCookie;
	}

	public boolean isCluster() {
		return isCluster;
	}

	public void setCluster(boolean isCluster) {
		this.isCluster = isCluster;
	}

	public String getVBrokerId() {
		return vbrokerId;
	}

	public void setVBrokerId(String vbrokerId) {
		this.vbrokerId = vbrokerId;
	}

	public String getVBrokerName() {
		return vbrokerName;
	}

	public void setVBrokerName(String vbrokerName) {
		this.vbrokerName = vbrokerName;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	@Override
	public String toString() {
		return "Broker [brokerId=" + brokerId + ", brokerName=" + brokerName + ", hostName=" + hostName + ", ip=" + ip + ", vip=" + vip
				+ ", port=" + port + ", mgrPort=" + mgrPort + ", mqUser=" + mqUser + ", mqPwd=" + mqPwd + ", vhost=" + vhost
				+ ", erlCookie=" + erlCookie + ", isCluster=" + isCluster + ", vbrokerId=" + vbrokerId + ", vbrokerName=" + vbrokerName
				+ ", groupId=" + groupId + ", groupName=" + groupName + "]";
	}

}
