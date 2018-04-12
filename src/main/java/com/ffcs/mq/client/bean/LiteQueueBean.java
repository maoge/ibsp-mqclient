package com.ffcs.mq.client.bean;

import com.ffcs.mq.client.utils.CONSTS;

public class LiteQueueBean {
	
	private int type;               // queue:1; topic:2
	private int topicType;          // 当type = 2 时, 区分匿名、永久、通配, Anonymous:1; Permnent:2; Wildcard:3
	private String srcName;         // name of queue or topic
	private String mainKey;
	private String subKey;
	private String consumerID;
	
	public LiteQueueBean(int type, int topicType, String srcName,
			String mainKey, String subKey, String consumerID) {
		super();
		this.type = type;
		this.topicType = topicType;
		this.srcName = srcName;
		this.mainKey = mainKey;
		this.subKey = subKey;
		this.consumerID = consumerID;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getTopicType() {
		return topicType;
	}

	public void setTopicType(int topicType) {
		this.topicType = topicType;
	}

	public String getSrcName() {
		return srcName;
	}

	public void setSrcName(String srcName) {
		this.srcName = srcName;
	}

	public String getMainKey() {
		return mainKey;
	}

	public void setMainKey(String mainKey) {
		this.mainKey = mainKey;
	}

	public String getSubKey() {
		return subKey;
	}

	public void setSubKey(String subKey) {
		this.subKey = subKey;
	}

	public String getConsumerID() {
		return consumerID;
	}

	public void setConsumerID(String consumerID) {
		this.consumerID = consumerID;
	}
	
	public String getName() {
		String s = null;
		switch (type) {
		case CONSTS.TYPE_QUEUE:
			s = srcName;
			break;
		case CONSTS.TYPE_TOPIC:
			switch (topicType) {
			case CONSTS.TOPIC_ANONYMOUS:
				s = srcName;
				break;
			case CONSTS.TOPIC_PERMERNENT:
			case CONSTS.TOPIC_WILDCARD:
				s = consumerID;
				break;
			default:
				break;
			}
			break;
		default:
			break;
		}
		
		return s;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((consumerID == null) ? 0 : consumerID.hashCode());
		result = prime * result + ((mainKey == null) ? 0 : mainKey.hashCode());
		result = prime * result + ((srcName == null) ? 0 : srcName.hashCode());
		result = prime * result + ((subKey == null) ? 0 : subKey.hashCode());
		result = prime * result + topicType;
		result = prime * result + type;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LiteQueueBean other = (LiteQueueBean) obj;
		if (consumerID == null) {
			if (other.consumerID != null)
				return false;
		} else if (!consumerID.equals(other.consumerID))
			return false;
		if (mainKey == null) {
			if (other.mainKey != null)
				return false;
		} else if (!mainKey.equals(other.mainKey))
			return false;
		if (srcName == null) {
			if (other.srcName != null)
				return false;
		} else if (!srcName.equals(other.srcName))
			return false;
		if (subKey == null) {
			if (other.subKey != null)
				return false;
		} else if (!subKey.equals(other.subKey))
			return false;
		if (topicType != other.topicType)
			return false;
		if (type != other.type)
			return false;
		return true;
	}

}
