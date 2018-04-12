package test;

public class NanoTimeTest {

	public static void main(String[] args) {
		long t1 = System.nanoTime();
		long t2 = System.nanoTime();
		long l = t2 - t1;
		
		String s = String.format("t1:%d, t2:%d, l:%d", t1, t2, l);
		System.out.println(s);
	}

}
