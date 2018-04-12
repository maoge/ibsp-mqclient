package test;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class ByteArrToJson {

	public static void main(String[] args) {
		JSONObject json = new JSONObject();
		json.put("abc", "123");
		
		String s = json.toString();
		System.out.println(s);
		
		byte[] bs = s.getBytes();
		
		CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
		
		JSONObject newJson = (JSONObject) JSONObject.parse(bs, 0, bs.length, decoder, JSON.DEFAULT_PARSER_FEATURE);
		
		String val = newJson.getString("abc");
		System.out.println(val);
	}

}
