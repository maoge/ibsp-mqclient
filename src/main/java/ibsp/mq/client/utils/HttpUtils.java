package ibsp.mq.client.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUtils {

	private static Logger logger = LoggerFactory.getLogger(HttpUtils.class);
	private static final int READ_TIMEOUT = 3000;
	private static final int CONN_TIMEOUT = 3000;

	public static boolean getData(String urlStr, SVarObject sVar) {
		String res = null;
		URL url = null;
		StringBuffer strBuff = null;
		BufferedReader buffReader = null;
		HttpURLConnection urlConn = null;

		boolean isConn = false;
		boolean ok = true;

		try {
			url = new URL(urlStr);
			urlConn = (HttpURLConnection) url.openConnection();

			urlConn.setRequestMethod(CONSTS.HTTP_METHOD_GET);
			urlConn.setDoOutput(true);
			urlConn.setReadTimeout(READ_TIMEOUT);
			urlConn.setConnectTimeout(CONN_TIMEOUT);
			urlConn.setDefaultUseCaches(false);
			urlConn.setUseCaches(false);
			urlConn.setRequestProperty("Accept-Charset", "UTF-8");
			urlConn.setRequestProperty("Connection", "keep-alive");
			isConn = true;

			int respCode = urlConn.getResponseCode();
			if (respCode == HttpURLConnection.HTTP_OK) {
				String s = null;
				strBuff = new StringBuffer();

				buffReader = new BufferedReader(new InputStreamReader(urlConn.getInputStream()));
				while ((s = buffReader.readLine()) != null) {
					strBuff.append(s);
				}

				res = strBuff.toString();
				strBuff.setLength(0);

				sVar.setVal(res);
			}

		} catch (Exception e) {
			logger.error("读取:{}异常. error:{}", urlStr, e);
			Global.get().setLastError(e.getMessage());

			res = null;
			ok = false;
		} finally {
			if (buffReader != null) {
				try {
					buffReader.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}

			if (urlConn != null && isConn) {
				urlConn.disconnect();
			}
		}

		return ok;
	}

	public static boolean postData(String urlStr, String params, SVarObject sVar) {
		String res = null;
		URL url = null;
		StringBuffer strBuff = null;
		BufferedReader buffReader = null;
		HttpURLConnection urlConn = null;

		boolean isConn = false;
		boolean ok = true;

		try {
			url = new URL(urlStr);
			urlConn = (HttpURLConnection) url.openConnection();

			urlConn.setRequestMethod(CONSTS.HTTP_METHOD_POST);
			urlConn.setDoOutput(true);
			urlConn.setReadTimeout(READ_TIMEOUT);
			urlConn.setConnectTimeout(CONN_TIMEOUT);
			urlConn.setDefaultUseCaches(false);
			urlConn.setUseCaches(false);
			urlConn.setRequestProperty("Accept-Charset", "UTF-8");
			urlConn.setRequestProperty("Connection", "keep-alive");
			isConn = true;

			byte[] bypes = params.toString().getBytes();
			urlConn.getOutputStream().write(bypes);

			String s = null;
			strBuff = new StringBuffer();

			buffReader = new BufferedReader(new InputStreamReader(urlConn.getInputStream()));
			while ((s = buffReader.readLine()) != null) {
				strBuff.append(s);
			}

			res = strBuff.toString();
			strBuff.setLength(0);

			sVar.setVal(res);
				
			ok = urlConn.getResponseCode() == HttpURLConnection.HTTP_OK;

		} catch (Exception e) {
			logger.error("读取:{}异常. error:{}", urlStr, e);
			Global.get().setLastError(e.getMessage());

			res = null;
			ok = false;
		} finally {
			if (buffReader != null) {
				try {
					buffReader.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}

			if (urlConn != null && isConn) {
				urlConn.disconnect();
			}
		}

		return ok;
	}

	public static void main(String[] args) {
		String url = "http://192.168.14.205:9991/configsvr/getQueueByName";
		String params = String.format("%s=%s", "qname", "LBTEST_00");
		SVarObject sVar = new SVarObject();
		boolean ret = HttpUtils.postData(url, params, sVar);
		System.out.println("ret:"+ ret + ", json:" + sVar.getVal());
	}

}
