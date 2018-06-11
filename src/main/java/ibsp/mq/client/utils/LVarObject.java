package ibsp.mq.client.utils;

public class LVarObject {
	
	private volatile long val;
	
	public LVarObject() {
		val = 0L;
	}

	public LVarObject(long i) {
		this.val = i;
	}

	public long getVal() {
		return val;
	}

	public void setVal(long val) {
		this.val = val;
	}
	
	public long incAndGet() {
		return ++val;
	}
	
	public long decAndGet() {
		return --val;
	}
	
	public long getAndInc() {
		return val++;
	}
	
	public long getAndDec() {
		return val--;
	}

	public void clear() {
		this.val = 0L;
	}

}
