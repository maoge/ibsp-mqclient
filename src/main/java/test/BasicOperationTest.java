package test;

import com.ffcs.mq.client.utils.BasicOperation;
import com.ffcs.mq.client.utils.SVarObject;

public class BasicOperationTest {

	public static void main(String[] args) {
		for (int i = 0; i < 100; i++) {
			SVarObject sVar = new SVarObject();
			BasicOperation.loadQueueByName("TEST_00", sVar);
		}
	}

}
