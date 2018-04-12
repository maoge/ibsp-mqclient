package test;

import com.ffcs.mq.client.exception.ZKLockException;
import com.ffcs.mq.client.utils.ZKLocker;

public class ZKLockerTest {

	public static void main(String[] args) {
		ZKLocker locker = new ZKLocker();
		try {
			boolean ret1 = locker.lock("TT_00");
			System.out.println("lock TT_00 return:" + ret1);
			
			boolean ret2 = locker.lock("TT_01");
			System.out.println("lock TT_01 return:" + ret2);
		} catch (ZKLockException e1) {
			e1.printStackTrace();
		}
		
		System.out.println("locked ......");
		try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		try {
			locker.unlock("TT_00");
			System.out.println("unlocked TT_00 ......");
		} catch(ZKLockException e) {
			e.printStackTrace();
		}
		
		try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("locker destroy ......");
		locker.destroy();
		
		try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}

}
