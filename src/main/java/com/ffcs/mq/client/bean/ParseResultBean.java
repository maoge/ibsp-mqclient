package com.ffcs.mq.client.bean;

public class ParseResultBean {

	private boolean ok;
	private String errInfo;

	public ParseResultBean() {
		super();
		ok = false;
		errInfo = "";
	}

	public boolean isOk() {
		return ok;
	}

	public void setOk(boolean ok) {
		this.ok = ok;
	}

	public String getErrInfo() {
		return errInfo;
	}

	public void setErrInfo(String errInfo) {
		this.errInfo = errInfo;
	}

}
