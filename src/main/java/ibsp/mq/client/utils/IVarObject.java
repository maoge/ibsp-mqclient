package ibsp.mq.client.utils;

public class IVarObject {

	private volatile int val;

	public IVarObject() {
		val = 0;
	}

	public IVarObject(int i) {
		this.val = i;
	}

	public int getVal() {
		return val;
	}

	public void setVal(int val) {
		this.val = val;
	}
	
	public int incAndGet() {
		return ++val;
	}
	
	public int decAndGet() {
		return --val;
	}
	
	public int getAndInc() {
		return val++;
	}
	
	public int getAndDec() {
		return val--;
	}

	public void clear() {
		this.val = 0;
	}
}
