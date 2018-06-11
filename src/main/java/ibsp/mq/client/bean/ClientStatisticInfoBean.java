package ibsp.mq.client.bean;

import java.util.concurrent.atomic.AtomicLong;

import ibsp.mq.client.api.MQMessage;
import ibsp.mq.client.utils.CONSTS;

public class ClientStatisticInfoBean {

	private AtomicLong proCnt; // 当前发送总数
	private AtomicLong conCnt; // 当前消费总数
	private AtomicLong oldProCnt; // 最后一次统计后的发送总数
	private AtomicLong oldConCnt; // 最后一次统计后的消费总数
	private AtomicLong proByte; // 当前发送总字节数
	private AtomicLong conByte; // 当前消费总字节数

	private volatile long proTps; // 最后一次统计发送TPS
	private volatile long conTps; // 最后一次统计消费TPS
	private volatile long lastTime; // 最后一次统计时间错

	public ClientStatisticInfoBean() {
		proCnt = new AtomicLong(0L);
		conCnt = new AtomicLong(0L);
		oldProCnt = new AtomicLong(0L);
		oldConCnt = new AtomicLong(0L);
		proByte = new AtomicLong(0L);
		conByte = new AtomicLong(0L);

		proTps = 0;
		conTps = 0;
		lastTime = System.currentTimeMillis();
	}

	public void clear() {
		proCnt.set(0L);
		conCnt.set(0L);
		oldProCnt.set(0L);
		oldConCnt.set(0L);
		proByte.set(0L);
		conByte.set(0L);

		proTps = 0;
		conTps = 0;
		lastTime = System.currentTimeMillis();
	}

	public int getClientType() {
		int type = CONSTS.TYPE_NULL;

		if (proCnt.get() > 0) {
			type |= CONSTS.TYPE_PRO;
		}

		if (conCnt.get() > 0) {
			type |= CONSTS.TYPE_CON;
		}

		return type;
	}

	public void incProCnt() {
		proCnt.incrementAndGet();
	}

	public void incConCnt() {
		conCnt.incrementAndGet();
	}

	public void incProByte(int length) {
		proByte.addAndGet(length);
	}

	public void incConByte(int length) {
		conByte.addAndGet(length);
	}

	public long getProCnt() {
		return proCnt.get();
	}

	public long getConCnt() {
		return conCnt.get();
	}

	public long getProByte() {
		return proByte.get();
	}

	public long getConByte() {
		return conByte.get();
	}

	public long getOldProCnt() {
		return oldProCnt.get();
	}

	public long getOldConCnt() {
		return oldConCnt.get();
	}

	public long getProTPS() {
		return proTps;
	}

	public long getConTPS() {
		return conTps;
	}

	public long getLastTime() {
		return lastTime;
	}

	private void doSwitch() {
		oldProCnt.set(proCnt.get());
		oldConCnt.set(conCnt.get());
	}

	public void computeTPS() {
		long currTs = System.currentTimeMillis();
		long ts = currTs - lastTime;
		if (ts > 0) {
			proTps = (proCnt.get() - oldProCnt.get()) * 1000 / ts;
			conTps = (conCnt.get() - oldConCnt.get()) * 1000 / ts;
	
			doSwitch();
			lastTime = currTs;
		}
	}

	public void collectStatisticInfo(MQMessage msg, int type) {
		if (msg == null)
			return;

		byte[] msgBody = msg.getBody();
		if (msgBody == null)
			return;

		if (type == CONSTS.TYPE_PRO) {
			incProCnt();
			incProByte(msgBody.length);
		} else {
			incConCnt();
			incConByte(msgBody.length);
		}
	}

}
