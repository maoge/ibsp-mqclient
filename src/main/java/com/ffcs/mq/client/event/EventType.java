package com.ffcs.mq.client.event;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum EventType {
	
	e0(50010000,   false, "default"),

	e51(50010051,  false, "stop send/publish msg on vbroker"),    // group缩容前要对即将移除的VBROKER停写
	e52(50010052,  false, "add vbroker to group"),                // group扩容
	e53(50010053,  false, "remove vbroker from group"),           // group缩容
	
	e54(50010054,  true,  "broker down"),                         // broker crashed
	e55(50010055,  false, "broker recovered"),                    // broker service is recovered
	e56(50010056,  true,  "ha cluster swithed");                  // master-slave have switched 

	private final int value;
	private final boolean alarm;
	private final String info;

	private static final Map<Integer, EventType> map = new HashMap<Integer, EventType>();

	static {
		for (EventType s : EnumSet.allOf(EventType.class)) {
			map.put(s.value, s);
		}
	}

	private EventType(int i, boolean b, String s) {
		value = i;
		alarm = b;
		info = s;
	}

	public static EventType get(int code) {
		return map.get(code);
	}

	public int getValue() {
		// 得到枚举值代表的字符串。
		return value;
	}

	public boolean isAarm() {
		return alarm;
	}

	public String getInfo() {
		// 得到枚举值代表的字符串。
		return info;
	}

	public boolean equals(EventType e) {
		return this.value == e.value;
	}

}
