package com.ffcs.mq.client.router;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ffcs.mq.client.utils.CONSTS;
import com.ffcs.mq.client.utils.StringUtils;

public class Queue {

	private String queueId;
	private String queueName;
	private String type; // queue:"1"; topic:"2"
	private boolean durable; // 持久化
	private boolean ordered; // 全局有序
	private boolean deployed; // 是否已经创建发布

	private String groupId;
	private String groupName;

	public Queue(String queueId, String queueName, String type, boolean durable,
			boolean ordered, boolean deployed, String groupId, String groupName) {
		super();

		this.queueId = queueId;
		this.queueName = queueName;
		this.type = type;
		this.durable = durable;
		this.ordered = ordered;
		this.deployed = deployed;
		this.groupId = groupId;
		this.groupName = groupName;
	}

	public String getQueueId() {
		return queueId;
	}

	public void setQueueId(String queueId) {
		this.queueId = queueId;
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public boolean isDurable() {
		return durable;
	}

	public void setDurable(boolean durable) {
		this.durable = durable;
	}
	
	public boolean isOrdered() {
		return ordered;
	}

	public void setOrdered(boolean ordered) {
		this.ordered = ordered;
	}

	public boolean isDeployed() {
		return deployed;
	}

	public void setDeployed(boolean deployed) {
		this.deployed = deployed;
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

	public static Queue fromJson(JSONObject jsonObj) {
		
		if (jsonObj.isEmpty())
			return null;

		String queueId = (String) jsonObj.get(CONSTS.JSON_HEADER_QUEUE_ID);
		String queueName = (String) jsonObj.get(CONSTS.JSON_HEADER_QUEUE_NAME);
		String queueType = (String) jsonObj.get(CONSTS.JSON_HEADER_QUEUE_TYPE);
		boolean durable = ((String) jsonObj.get(CONSTS.JSON_HEADER_IS_DURABLE)).equals(CONSTS.DURABLE);
		boolean ordered = ((String) jsonObj.get(CONSTS.JSON_HEADER_IS_ORDERED)).equals(CONSTS.ORDERED);
		boolean deployed = ((String) jsonObj.get(CONSTS.JSON_HEADER_IS_DEPLOY)).equals(CONSTS.DEPLOYED);
		String groupId = (String) jsonObj.get(CONSTS.JSON_HEADER_SERV_ID);
		String groupName = (String) jsonObj.get(CONSTS.JSON_HEADER_SERV_NAME);

		return new Queue(queueId, queueName, queueType, durable, ordered, deployed, groupId, groupName);
	}
	
	@Override
	public String toString() {
		return "Queue [queueId=" + queueId + ", queueName=" + queueName
				+ ", type=" + type + ", durable=" + durable + ", ordered=" + ordered
				+ ", deployed=" + deployed + ", groupId=" + groupId + ", groupName=" + groupName + "]";
	}

}
