package ibsp.mq.client.utils;

import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockingLock<T> {

	private static Logger logger = LoggerFactory.getLogger(BlockingLock.class);

	private boolean _filled = false;
	private T _value;
	private static final long NANOS_IN_MILLI = 1000 * 1000;
	private static final long INFINITY = -1;

	public BlockingLock() {

	}

	public synchronized T get() throws InterruptedException {
		while (!_filled) {
			wait();
		}

		return _value;
	}

	public synchronized T get(long timeout) throws InterruptedException, TimeoutException {
		if (timeout == INFINITY)
			return get();

		if (timeout < 0)
			throw new AssertionError("Timeout cannot be less than zero");

		long maxTime = System.currentTimeMillis() + timeout;
		long now;
		while (!_filled && (now = System.currentTimeMillis()) < maxTime) {
			wait(maxTime - now);
		}

		if (!_filled)
			throw new TimeoutException();

		return _value;
	}

	public synchronized T uninterruptibleGet() {
		while (true) {
			try {
				return get();
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	public synchronized T uninterruptibleGet(int timeout) throws TimeoutException {
		long now = System.nanoTime() / NANOS_IN_MILLI;
		long runTime = now + timeout;

		do {
			try {
				return get(runTime - now);
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		} while ((timeout == INFINITY) || ((now = System.nanoTime() / NANOS_IN_MILLI) < runTime));

		throw new TimeoutException();
	}

	public synchronized void set(T newValue) {
		if (_filled) {
			// throw new AssertionError("BlockingCell can only be set once");
			return;
		}
		_value = newValue;
		_filled = true;
		notifyAll();
	}

	public synchronized boolean setIfUnset(T newValue) {
		if (_filled) {
			return false;
		}
		set(newValue);
		return true;
	}

	public void setFilled(boolean fill) {
		this._filled = fill;
	}

}
