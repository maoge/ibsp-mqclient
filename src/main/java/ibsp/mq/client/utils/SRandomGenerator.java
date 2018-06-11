package ibsp.mq.client.utils;

import java.util.Random;
import java.util.UUID;

public class SRandomGenerator {

	private static int TAG_LEN = 15;
	private static int ID_LEN = 16;

	private static char[] DICT_TABLE = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
			'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
			'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z' };

	public static String genConsumerTag() {
		int len = DICT_TABLE.length - 1;
		char[] buf = new char[TAG_LEN];
		Random random = new Random();

		for (int i = 0; i < TAG_LEN; i++) {
			int index = random.nextInt(len) % len;
			buf[i] = DICT_TABLE[index];
		}

		return String.format("%s_%s", CONSTS.GEN_TAG_PREFIX, new String(buf));
	}

	public static String genConsumerID() {
		int len = DICT_TABLE.length - 1;
		char[] buf = new char[ID_LEN];
		Random random = new Random();

		for (int i = 0; i < ID_LEN; i++) {
			int index = random.nextInt(len) % len;
			buf[i] = DICT_TABLE[index];
		}

		return String.format("%s_%s", CONSTS.PERM_CON_PREFIX, new String(buf));
	}
	
	public static String genUUID() {
		UUID uuid = UUID.randomUUID();
		return uuid.toString();
	}

}
