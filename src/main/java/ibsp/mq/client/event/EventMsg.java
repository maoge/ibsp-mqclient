package ibsp.mq.client.event;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import ibsp.mq.client.utils.CONSTS;
import ibsp.mq.client.utils.StringUtils;

public class EventMsg {

	private static Logger logger = LoggerFactory.getLogger(EventMsg.class);
	private static CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

	private int eventCode;
	private String queueId;
	private String queueName;
	private String groupId;
	private String vbrokerId;
	private String brokerId;
	private String jsonStr;
	
	private static Class<?> CLAZZ;
	
	static {
		CLAZZ = EventMsg.class;
	}

	public EventMsg() {
		eventCode = 0;
		queueId   = "";
		queueName = "";
		groupId   = "";
		vbrokerId = "";
		brokerId  = "";
		jsonStr   = "";
	}

	public int getEventCode() {
		return eventCode;
	}

	public void setEventCode(int eventCode) {
		this.eventCode = eventCode;
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

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public String getVBrokerId() {
		return vbrokerId;
	}

	public void setVBrokerId(String vbrokerId) {
		this.vbrokerId = vbrokerId;
	}

	public String getBrokerId() {
		return brokerId;
	}

	public void setBrokerId(String brokerId) {
		this.brokerId = brokerId;
	}
	
	public String getJsonStr() {
		return jsonStr;
	}

	public void setJsonStr(String jsonStr) {
		this.jsonStr = jsonStr;
	}

	public static EventMsg fronJson(String sEventInfo) {
		JSONObject json = null;
		try {
			json = JSONObject.parseObject(sEventInfo);
		} catch(JSONException e) {
			logger.error("json:{} parse error", sEventInfo);
			logger.error(e.getMessage(), e);
		}
		
		if (json == null)
			return null;

		int code = json.getInteger(CONSTS.EV_CODE);
		String queueId = json.getString(CONSTS.EV_QUEUE_ID);
		String queueName = json.getString(CONSTS.EV_QUEUE_NAME);
		String groupId = json.getString(CONSTS.EV_GROUP_ID);
		String brokerId = json.getString(CONSTS.EV_BROKER_ID);
		String vbrokerId = json.getString(CONSTS.EV_VBROKER_ID);

		if (code < EventType.e0.getValue()) {
			logger.error("event:{} illegal.", sEventInfo);
			return null;
		}

		EventMsg event = new EventMsg();
		event.setEventCode(code);

		if (!StringUtils.isNullOrEmtpy(queueId)) {
			event.setQueueId(queueId);
		}

		if (!StringUtils.isNullOrEmtpy(queueName)) {
			event.setQueueName(queueName);
		}

		if (!StringUtils.isNullOrEmtpy(groupId)) {
			event.setGroupId(groupId);
		}

		if (!StringUtils.isNullOrEmtpy(brokerId)) {
			event.setBrokerId(brokerId);
		}

		if (!StringUtils.isNullOrEmtpy(vbrokerId)) {
			event.setVBrokerId(vbrokerId);
		}

		return event;
	}
	
	public static EventMsg fronJson(byte[] bs, int offset, int len) {
		JSONObject json = null;
		try {
			json = (JSONObject) JSONObject.parse(bs, offset, len, decoder, JSON.DEFAULT_PARSER_FEATURE);
		} catch(JSONException e) {
			logger.error("json:{} parse error", bs.toString());
			logger.error(e.getMessage(), e);
		}
		
		if (json == null)
			return null;

		int code = json.getInteger(CONSTS.EV_CODE);
		String queueId = json.getString(CONSTS.EV_QUEUE_ID);
		String queueName = json.getString(CONSTS.EV_QUEUE_NAME);
		String groupId = json.getString(CONSTS.EV_GROUP_ID);
		String brokerId = json.getString(CONSTS.EV_BROKER_ID);
		String vbrokerId = json.getString(CONSTS.EV_VBROKER_ID);
		String jsonStr = json.getString(CONSTS.EV_JSONSTR);

		if (code < EventType.e0.getValue()) {
			logger.error("json:{} parse error", bs.toString());
			return null;
		}

		EventMsg event = new EventMsg();
		event.setEventCode(code);

		if (!StringUtils.isNullOrEmtpy(queueId)) {
			event.setQueueId(queueId);
		}

		if (!StringUtils.isNullOrEmtpy(queueName)) {
			event.setQueueName(queueName);
		}

		if (!StringUtils.isNullOrEmtpy(groupId)) {
			event.setGroupId(groupId);
		}

		if (!StringUtils.isNullOrEmtpy(brokerId)) {
			event.setBrokerId(brokerId);
		}

		if (!StringUtils.isNullOrEmtpy(vbrokerId)) {
			event.setVBrokerId(vbrokerId);
		}
		
		if (!StringUtils.isNullOrEmtpy(jsonStr)) {
			event.setJsonStr(jsonStr);
		}

		return event;
	}
	
	public JSONObject getJson() {
		JSONObject json = new JSONObject();
		
		Object obj = null;
		Field[] fields = CLAZZ.getDeclaredFields();
		
		for (Field field : fields) {
			if (Modifier.isStatic(field.getModifiers()))
				continue;
			
			try {
				field.setAccessible(true);
				obj = field.get(this);
				
				if (obj == null)
					continue;
				
				String name = field.getName();
				
				if (obj instanceof EventType)
					json.put(name, ((EventType)obj).getValue());
				else
					json.put(name, obj);
			
			} catch (IllegalArgumentException e) {
				logger.error(e.getMessage(), e);
			} catch (IllegalAccessException e) {
				logger.error(e.getMessage(), e);
			}
		}
		
		return json;
	}
	
	public String toString() {
		return getJson().toString();
	}

}
