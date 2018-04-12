package com.ffcs.mq.client.utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

public class BasicOperation {

	private static Logger logger = LoggerFactory.getLogger(BasicOperation.class);

	public static boolean auth(String userId, String userPwd) {
		boolean ret = false;
			
		if (Global.get().isAuthed())
			return true;
		
		if (StringUtils.isNullOrEmtpy(userId) || StringUtils.isNullOrEmtpy(userPwd)) {
			String err = String.format("userId or userPwd is null or null string, need setAuthInfo to identify.");
			logger.error(err);
			return false;
		}
		
		String rootUrl = Global.get().getNextUrl();
		if (StringUtils.isNullOrEmtpy(rootUrl))
			return false;
		
		SVarObject sVar = new SVarObject();
		String reqUrl   = String.format("%s/%s/%s", rootUrl, CONSTS.CONFIGSVR, CONSTS.FUN_URL_AUTH);
		String reqParam = String.format("%s=%s&%s=%s", CONSTS.PARAM_USER_ID, userId, CONSTS.PARAM_USER_PWD, userPwd);
		
		if (!HttpUtils.postData(reqUrl, reqParam, sVar)) {
			logger.error("http request:{} error.", reqUrl);
			logger.error(Global.get().getLastError());
			Global.get().putBrokenUrl(rootUrl);
			return ret;
		}
		
		JSONObject jsonObj = JSONObject.parseObject(sVar.getVal());
		int retCode    = jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE);
		if (retCode == CONSTS.REVOKE_OK) {
			String magicKey = jsonObj.getString(CONSTS.JSON_HEADER_MAGIC_KEY);
			
			if (magicKey != null && !magicKey.isEmpty()) {
				Global.get().setAuthed(true);
				Global.get().setMagicKey(magicKey);
				ret = true;
			}
		} else {
			String retInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
			String err = String.format("auth fail, %s", retInfo);
			logger.error(err);
			Global.get().setLastError(err);
		}
			
		return ret;
	}

	/**
	 * 
	 * @param queueName
	 *            in
	 * @param sVar
	 *            out "return http respond json string"
	 * @return
	 */
	public static boolean loadQueueByName(String queueName, SVarObject sVar) {
		boolean ret = false;

		String rootUrl = Global.get().getNextUrl();
		if (StringUtils.isNullOrEmtpy(rootUrl))
			return ret;
		
		if (!Global.get().isAuthed()) {
			if (!auth(SysConfig.get().getMqUserId(), SysConfig.get().getMqUserPwd())) {
				return ret;
			}
		}
		
		String reqUrl = String.format("%s/%s/%s", rootUrl, CONSTS.CONFIGSVR, CONSTS.FUN_GETQUEUEBYQNAME);
		String reqParam = String.format("%s=%s&%s=%s", CONSTS.PARAM_QNAME, queueName, CONSTS.PARAM_MAGIC_KEY, Global.get().getMagicKey());

		boolean retPost = HttpUtils.postData(reqUrl, reqParam, sVar);
		if (retPost) {
			JSONObject json = JSONObject.parseObject(sVar.getVal());
			int retCode = json.getInteger(CONSTS.JSON_HEADER_RET_CODE);
			if (retCode == CONSTS.REVOKE_OK) {
				ret = true;
			} else if (retCode == CONSTS.REVOKE_AUTH_FAIL) {
				Global.get().clearAuth();
				Global.get().setLastError(CONSTS.ERR_AUTH_FAIL);
			} else {
				String errInfo = json.getString(CONSTS.JSON_HEADER_RET_INFO);
				Global.get().setLastError(errInfo);
			}
		} else {
			logger.error("http request:{} error.", reqUrl);
			logger.error(Global.get().getLastError());
			Global.get().putBrokenUrl(rootUrl);
		}

		return ret;
	}

	/**
	 * 
	 * @param queueName
	 *            in
	 * @param sVar
	 *            out "return http respond json string"
	 * @return
	 */
	public static boolean loadQueueBrokerRealtion(String queueName, SVarObject sVar) {
		boolean ret = false;

		String rootUrl = Global.get().getNextUrl();
		if (StringUtils.isNullOrEmtpy(rootUrl))
			return ret;
		
		if (!Global.get().isAuthed()) {
			if (!auth(SysConfig.get().getMqUserId(), SysConfig.get().getMqUserPwd())) {
				return ret;
			}
		}

		String reqUrl = String.format("%s/%s/%s", rootUrl, CONSTS.CONFIGSVR, CONSTS.FUN_GETBORKERSBYQNAME);
		String reqParam = String.format("%s=%s&%s=%s", CONSTS.PARAM_QNAME, queueName, CONSTS.PARAM_MAGIC_KEY, Global.get().getMagicKey());

		boolean retPost = HttpUtils.postData(reqUrl, reqParam, sVar);
		if (retPost) {
			JSONObject json = JSONObject.parseObject(sVar.getVal());
			int retCode = json.getInteger(CONSTS.JSON_HEADER_RET_CODE);
			if (retCode == CONSTS.REVOKE_OK) {
				ret = true;
			} else if (retCode == CONSTS.REVOKE_AUTH_FAIL) {
				Global.get().clearAuth();
				Global.get().setLastError(CONSTS.ERR_AUTH_FAIL);
			} else {
				String errInfo = json.getString(CONSTS.JSON_HEADER_RET_INFO);
				Global.get().setLastError(errInfo);
			}
		} else {
			logger.error("http request:{} error.", reqUrl);
			logger.error(Global.get().getLastError());
			Global.get().putBrokenUrl(rootUrl);
		}

		return ret;
	}

	/**
	 * 
	 * @param queueName
	 *            in "queue or topic to create"
	 * @param durable
	 *            in "durable flag"
	 * @param ordered
	 *            in "global ordered"
	 * @param groupId
	 *            in "group_id on which queue to created on"
	 * @param type
	 *            in "1:queue; 2:topic"
	 * @param sVar
	 *            out "return http respond json string"
	 * @return 0:ok; -1:nok; -2:nok queue exists
	 */
	public static int queueDeclare(String queueName, boolean durable, boolean ordered, String groupId, String type, SVarObject sVar) {
		int ret = CONSTS.REVOKE_NOK;

		if (StringUtils.isNullOrEmtpy(queueName)) {
			sVar.setVal("queueDeclare error: queueName is null or emputy.");
			return ret;
		}

		String rootUrl = Global.get().getNextUrl();
		if (!Global.get().isAuthed()) {
			if (!auth(SysConfig.get().getMqUserId(), SysConfig.get().getMqUserPwd())) {
				return ret;
			}
		}
		
		String reqUrl = String.format("%s/%s/%s", rootUrl, CONSTS.CONFIGSVR, CONSTS.FUN_CREATEQUEUEBYCLIENT);
		String reqParam = String.format("%s=%s&%s=%s&%s=%s&%s=%s&%s=%s&%s=%s",
				CONSTS.PARAM_QUEUENAME, queueName,
				CONSTS.PARAM_QUEUETYPE, type,
				CONSTS.PARAM_DURABLE, durable ? CONSTS.DURABLE : CONSTS.NOT_DURABLE,
				CONSTS.PARAM_ORDERED, ordered ? CONSTS.GLOBAL_ORDERED : CONSTS.NOT_GLOBAL_ORDERED,
				CONSTS.PARAM_GROUPID, groupId,
				CONSTS.PARAM_MAGIC_KEY, Global.get().getMagicKey());
		
		SVarObject sVarInvoke = new SVarObject();
		boolean retInvoke = HttpUtils.postData(reqUrl, reqParam, sVarInvoke);
		if (retInvoke) {
			JSONObject jsonObj = JSONObject.parseObject(sVarInvoke.getVal());
			int retCode = jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE);

			if (retCode != CONSTS.REVOKE_OK) {
				String retInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
				sVar.setVal(retInfo);
				
				if (retCode == CONSTS.REVOKE_AUTH_FAIL) {
					Global.get().clearAuth();
					Global.get().setLastError(CONSTS.ERR_AUTH_FAIL);
				} else {
					String errInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
					Global.get().setLastError(errInfo);
				}
			}
			
			ret = retCode;
		} else {
			sVar.setVal("queueDeclare:" + queueName + " http no response.");
			Global.get().putBrokenUrl(rootUrl);
			
			logger.error("http request:{} error.", reqUrl);
			logger.error(Global.get().getLastError());
		}

		return ret;
	}

	public static int queueDelete(String queueName, SVarObject sVar) {
		int ret = CONSTS.REVOKE_NOK;

		if (StringUtils.isNullOrEmtpy(queueName)) {
			sVar.setVal("queueName error: queueName is null or emputy.");
			return ret;
		}

		String rootUrl = Global.get().getNextUrl();
		if (!Global.get().isAuthed()) {
			if (!auth(SysConfig.get().getMqUserId(), SysConfig.get().getMqUserPwd())) {
				return ret;
			}
		}
		
		String reqUrl = String.format("%s/%s/%s", rootUrl, CONSTS.CONFIGSVR, CONSTS.FUN_DELETEQUEUEBYCLIENT);
		String reqParam = String.format("%s=%s&%s=%s", CONSTS.PARAM_QUEUENAME, queueName,
				CONSTS.PARAM_MAGIC_KEY, Global.get().getMagicKey());

		SVarObject sVarInvoke = new SVarObject();
		boolean retInvoke = HttpUtils.postData(reqUrl, reqParam, sVarInvoke);
		if (retInvoke) {
			JSONObject jsonObj = JSONObject.parseObject(sVarInvoke.getVal());
			int retCode = jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE);

			if (retCode != CONSTS.REVOKE_OK) {
				String retInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
				sVar.setVal(retInfo);
				
				if (retCode == CONSTS.REVOKE_AUTH_FAIL) {
					Global.get().clearAuth();
					Global.get().setLastError(CONSTS.ERR_AUTH_FAIL);
				} else {
					String errInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
					Global.get().setLastError(errInfo);
				}
			}

			ret = retCode;
		} else {
			sVar.setVal("queueDeclare:" + queueName + " http no response.");
			Global.get().putBrokenUrl(rootUrl);
			
			logger.error("http request:{} error.", reqUrl);
			logger.error(Global.get().getLastError());
		}

		return ret;
	}

	public static int getLocalIP(SVarObject sVarIP) {
		int ret = CONSTS.REVOKE_NOK;

		String rootUrl = Global.get().getNextUrl();
		String reqUrl = String.format("%s/%s/%s", rootUrl, CONSTS.CONFIGSVR, CONSTS.FUN_URL_TEST);

		SVarObject sVarInvoke = new SVarObject();
		boolean retInvoke = HttpUtils.getData(reqUrl, sVarInvoke);
		if (retInvoke) {
			JSONObject jsonObj = JSONObject.parseObject(sVarInvoke.getVal());
			ret = jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE);

			if (ret != CONSTS.REVOKE_OK) {
				String retInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
				logger.error("getLocalIP error:{}", retInfo);
			} else {
				String ip = jsonObj.getString(CONSTS.JSON_HEADER_REMOTE_IP);
				sVarIP.setVal(ip);
			}
		} else {
			logger.error("http request:{} error.", reqUrl);
			logger.error(Global.get().getLastError());
			Global.get().putBrokenUrl(rootUrl);
		}

		return ret;
	}

	@SuppressWarnings("resource")
	public static int getUsablePort(String ip, IVarObject iVarPort) {
		int ret = CONSTS.REVOKE_NOK;

		int basePort = CONSTS.BASE_PORT;
		int maxPort = basePort + CONSTS.BATCH_FIND_CNT;
		for (int port = basePort; port < maxPort; port++) {
			try {
				ServerSocket sock = new ServerSocket();
				InetSocketAddress addr = new InetSocketAddress(ip, port);
				sock.bind(addr);

				if (sock.isBound()) {
					sock.close();
					iVarPort.setVal(port);

					ret = CONSTS.REVOKE_OK;
					break;
				}

			} catch (IOException e) {
				continue;
			}
		}

		return ret;
	}

	public static int genConsumerID(SVarObject sVarID) {
		int ret = CONSTS.REVOKE_NOK;

		String rootUrl = Global.get().getNextUrl();
		if (!Global.get().isAuthed()) {
			if (!auth(SysConfig.get().getMqUserId(), SysConfig.get().getMqUserPwd())) {
				return ret;
			}
		}
		
		String reqUrl = String.format("%s/%s/%s", rootUrl, CONSTS.CONFIGSVR, CONSTS.FUN_GEN_CONSUMER_ID);
		String reqParam = String.format("%s=%s", CONSTS.PARAM_MAGIC_KEY, Global.get().getMagicKey());
		
		SVarObject sVarInvoke = new SVarObject();
		boolean retInvoke = HttpUtils.postData(reqUrl, reqParam, sVarInvoke);
		if (retInvoke) {
			JSONObject jsonObj = JSONObject.parseObject(sVarInvoke.getVal());
			ret = jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE);
			
			if (ret != CONSTS.REVOKE_OK) {
				logger.error(sVarInvoke.getVal());
				if (ret == CONSTS.REVOKE_AUTH_FAIL) {
					Global.get().clearAuth();
					Global.get().setLastError(CONSTS.ERR_AUTH_FAIL);
				} else {
					String errInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
					Global.get().setLastError(errInfo);
				}
			} else {
				String retConsumerID = jsonObj.getString(CONSTS.JSON_HEADER_CONSUMER_ID);
				if (!StringUtils.isNullOrEmtpy(retConsumerID)) {
					sVarID.setVal(retConsumerID);
				}
			}
		} else {
			logger.error("http request:{} error.", reqUrl);
			logger.error(Global.get().getLastError());
			Global.get().putBrokenUrl(rootUrl);
		}

		return ret;
	}

	public static int genPermQueue(SVarObject sVarQueue) {
		int ret = CONSTS.REVOKE_NOK;

		String rootUrl = Global.get().getNextUrl();
		if (!Global.get().isAuthed()) {
			if (!auth(SysConfig.get().getMqUserId(), SysConfig.get().getMqUserPwd())) {
				return ret;
			}
		}
		
		String reqUrl = String.format("%s/%s/%s", rootUrl, CONSTS.CONFIGSVR, CONSTS.FUN_GEN_PERM_QUEUE);
		String reqParam = String.format("%s=%s", CONSTS.PARAM_MAGIC_KEY, Global.get().getMagicKey());

		SVarObject sVarInvoke = new SVarObject();
		boolean retInvoke = HttpUtils.postData(reqUrl, reqParam, sVarInvoke);
		if (retInvoke) {
			JSONObject jsonObj = JSONObject.parseObject(sVarInvoke.getVal());
			ret = jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE);
			
			if (ret != CONSTS.REVOKE_OK) {
				logger.error(sVarInvoke.getVal());
				if (ret == CONSTS.REVOKE_AUTH_FAIL) {
					Global.get().clearAuth();
					Global.get().setLastError(CONSTS.ERR_AUTH_FAIL);
				} else {
					String errInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
					Global.get().setLastError(errInfo);
				}
			} else {
				String retPermQueue = jsonObj.getString(CONSTS.JSON_HEADER_PERM_QUEUE);
				if (!StringUtils.isNullOrEmtpy(retPermQueue)) {
					sVarQueue.setVal(retPermQueue);
				}
			}
		} else {
			logger.error("http request:{} error.", reqUrl);
			logger.error(Global.get().getLastError());
			Global.get().putBrokenUrl(rootUrl);
		}

		return ret;
	}

	public static int getPermnentTopic(String consumerId, SVarObject sVarSrcQueue, SVarObject sVarRealQueue, SVarObject sVarMainKey,
			SVarObject sVarSubKey, SVarObject sVarGroupId) {
		int ret = CONSTS.REVOKE_NOK;

		String rootUrl = Global.get().getNextUrl();
		if (!Global.get().isAuthed()) {
			if (!auth(SysConfig.get().getMqUserId(), SysConfig.get().getMqUserPwd())) {
				return ret;
			}
		}
		
		String reqUrl = String.format("%s/%s/%s", rootUrl, CONSTS.CONFIGSVR, CONSTS.FUN_GETPERMNENTTOPIC);
		String reqParam = String.format("%s=%s&%s=%s", CONSTS.JSON_HEADER_CONSUMER_ID, consumerId,
				CONSTS.PARAM_MAGIC_KEY, Global.get().getMagicKey());
		
		SVarObject sVarInvoke = new SVarObject();
		boolean retInvoke = HttpUtils.postData(reqUrl, reqParam, sVarInvoke);
		if (retInvoke) {
			JSONObject jsonObj = JSONObject.parseObject(sVarInvoke.getVal());
			ret = jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE);

			if (ret == CONSTS.REVOKE_OK) {
				JSONObject subJson = jsonObj.getJSONObject(CONSTS.JSON_HEADER_RET_INFO);
				String realQueue = subJson.getString(CONSTS.JSON_HEADER_REAL_QUEUE);
				String srcQueue = subJson.getString(CONSTS.JSON_HEADER_SRC_QUEUE);
				String mainKey = subJson.getString(CONSTS.JSON_HEADER_MAIN_TOPIC);
				String subKey = subJson.getString(CONSTS.JSON_HEADER_SUB_TOPIC);
				String groupId = subJson.getString(CONSTS.JSON_HEADER_GROUP_ID);

				sVarSrcQueue.setVal(srcQueue);
				sVarRealQueue.setVal(realQueue);
				sVarMainKey.setVal(mainKey);
				sVarSubKey.setVal(subKey);
				sVarGroupId.setVal(groupId);
			} else {
				String errInfo = String.format("getPermnentTopic error, consumerId:%s url:%s", consumerId, rootUrl);
				logger.error(errInfo);
				
				if (ret == CONSTS.REVOKE_AUTH_FAIL) {
					Global.get().clearAuth();
					Global.get().setLastError(CONSTS.ERR_AUTH_FAIL);
				} else {
					String err = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
					Global.get().setLastError(err);
				}
			}
		} else {
			logger.error("http request:{} error.", reqUrl);
			logger.error(Global.get().getLastError());
			Global.get().putBrokenUrl(rootUrl);
		}

		return ret;
	}

	public static int putPermnentTopic(String consumerId, String srcQueue, String realQueue, String mainKey, String subKey, String groupId,
			SVarObject sVar) {
		int ret = CONSTS.REVOKE_NOK;

		String rootUrl = Global.get().getNextUrl();
		if (!Global.get().isAuthed()) {
			if (!auth(SysConfig.get().getMqUserId(), SysConfig.get().getMqUserPwd())) {
				return ret;
			}
		}
		
		String reqUrl = String.format("%s/%s/%s", rootUrl, CONSTS.CONFIGSVR, CONSTS.FUN_PUTPERMNENTTOPIC);
		String reqParam = String.format("%s=%s&%s=%s&%s=%s&%s=%s&%s=%s&%s=%s&%s=%s&%s=%s",
				CONSTS.JSON_HEADER_CONSUMER_ID, consumerId,
				CONSTS.JSON_HEADER_SRC_QUEUE, srcQueue,
				CONSTS.JSON_HEADER_REAL_QUEUE, realQueue,
				CONSTS.JSON_HEADER_MAIN_TOPIC, mainKey,
				CONSTS.JSON_HEADER_SUB_TOPIC, subKey,
				CONSTS.JSON_HEADER_GROUP_ID, groupId,
				CONSTS.JSON_HEADER_CONSUMER_ID, consumerId,
				CONSTS.PARAM_MAGIC_KEY, Global.get().getMagicKey());
		
		SVarObject sVarInvoke = new SVarObject();
		boolean retInvoke = HttpUtils.postData(reqUrl, reqParam, sVarInvoke);
		if (retInvoke) {
			JSONObject jsonObj = JSONObject.parseObject(sVarInvoke.getVal());
			ret = jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE);
			
			if (ret != CONSTS.REVOKE_OK) {
				String retInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
				sVar.setVal(retInfo);
				logger.error(retInfo);
				
				if (ret == CONSTS.REVOKE_AUTH_FAIL) {
					Global.get().clearAuth();
					Global.get().setLastError(CONSTS.ERR_AUTH_FAIL);
				} else {
					String errInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
					Global.get().setLastError(errInfo);
				}
			}
		} else {
			logger.error("http request:{} error.", reqUrl);
			logger.error(Global.get().getLastError());
			Global.get().putBrokenUrl(rootUrl);
		}

		return ret;
	}

	public static int delPermnentTopic(String consumerId, SVarObject sVar) {
		int ret = CONSTS.REVOKE_NOK;

		String rootUrl = Global.get().getNextUrl();
		if (!Global.get().isAuthed()) {
			if (!auth(SysConfig.get().getMqUserId(), SysConfig.get().getMqUserPwd())) {
				return ret;
			}
		}
		
		String reqUrl = String.format("%s/%s/%s", rootUrl, CONSTS.CONFIGSVR, CONSTS.FUN_DELPERMNENTTOPIC);
		String reqParam = String.format("%s=%s&%s=%s", CONSTS.JSON_HEADER_CONSUMER_ID, consumerId,
				CONSTS.PARAM_MAGIC_KEY, Global.get().getMagicKey());
		
		SVarObject sVarInvoke = new SVarObject();
		boolean retInvoke = HttpUtils.postData(reqUrl, reqParam, sVarInvoke);
		if (retInvoke) {
			JSONObject jsonObj = JSONObject.parseObject(sVarInvoke.getVal());
			ret = jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE);
			
			if (ret != CONSTS.REVOKE_OK) {
				String retInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
				sVar.setVal(retInfo);
				logger.error(retInfo);
				
				if (ret == CONSTS.REVOKE_AUTH_FAIL) {
					Global.get().clearAuth();
					Global.get().setLastError(CONSTS.ERR_AUTH_FAIL);
				} else {
					String errInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
					Global.get().setLastError(errInfo);
				}
			}
		} else {
			logger.error("http request:{} error.", reqUrl);
			logger.error(Global.get().getLastError());
			Global.get().putBrokenUrl(rootUrl);
		}

		return ret;
	}

	// FUN_PUT_CLNT_STAT_INFO
	public static int putClientStatisticInfo(String context, String lsnrAddr) {
		int ret = CONSTS.REVOKE_NOK;

		String rootUrl = Global.get().getNextUrl();
		if (!Global.get().isAuthed()) {
			if (!auth(SysConfig.get().getMqUserId(), SysConfig.get().getMqUserPwd())) {
				return ret;
			}
		}
		
		String reqUrl = String.format("%s/%s/%s", rootUrl, CONSTS.CONFIGSVR, CONSTS.FUN_PUT_CLNT_STAT_INFO);
		String params = String.format("%s=%s&%s=%s&%s=%s", CONSTS.PARAM_CLIENTINFO, context,
				CONSTS.PARAM_LSNRADDR, lsnrAddr,
				CONSTS.PARAM_MAGIC_KEY, Global.get().getMagicKey());

		SVarObject sVarInvoke = new SVarObject();
		boolean retInvoke = HttpUtils.postData(reqUrl, params, sVarInvoke);
		if (retInvoke) {
			JSONObject jsonObj = JSONObject.parseObject(sVarInvoke.getVal());
			ret = jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE);
			if (ret != CONSTS.REVOKE_OK) {
				String retInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
				logger.error("PutClientStatisticInfo error:{}", retInfo);
				
				if (ret == CONSTS.REVOKE_AUTH_FAIL) {
					Global.get().clearAuth();
					Global.get().setLastError(CONSTS.ERR_AUTH_FAIL);
				} else {
					String errInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
					Global.get().setLastError(errInfo);
				}
			}
		} else {
			logger.error("http request:{} error.", reqUrl);
			logger.error(Global.get().getLastError());
			Global.get().putBrokenUrl(rootUrl);
		}

		return ret;
	}
	
	public static int getMessageReady(String name) {
		//FUN_URL_MESSAGE_READY
		int ret = 0;
			
		String rootUrl = Global.get().getNextUrl();
		if (!Global.get().isAuthed()) {
			if (!auth(SysConfig.get().getMqUserId(), SysConfig.get().getMqUserPwd())) {
				return ret;
			}
		}
		
		String reqUrl = String.format("%s/%s/%s", rootUrl, CONSTS.CONFIGSVR, CONSTS.FUN_URL_MESSAGE_READY);
		String params = String.format("%s=%s&%s=%s", CONSTS.PARAM_NAME, name,
				CONSTS.PARAM_MAGIC_KEY, Global.get().getMagicKey());
		
		SVarObject sVarInvoke = new SVarObject();
		boolean retInvoke = HttpUtils.postData(reqUrl, params, sVarInvoke);
		if (retInvoke) {
			JSONObject jsonObj = JSONObject.parseObject(sVarInvoke.getVal());
			int retCode = jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE);
			if (retCode == CONSTS.REVOKE_OK) {
				ret = jsonObj.getIntValue(CONSTS.JSON_HEADER_MSG_READY);
			} else {
				String retInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
				logger.error("FUN_URL_MESSAGE_READY error:{}", retInfo);
				
				if (retCode == CONSTS.REVOKE_AUTH_FAIL) {
					Global.get().clearAuth();
					Global.get().setLastError(CONSTS.ERR_AUTH_FAIL);
				} else {
					String errInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
					Global.get().setLastError(errInfo);
				}
			} 
		} else {
			logger.error("http request:{} error.", reqUrl);
			logger.error(Global.get().getLastError());
			Global.get().putBrokenUrl(rootUrl);
		}
		
		return ret;
	}
	
	public static int revokePurgeQueue(String name, int type, SVarObject sVar) {
		int ret = CONSTS.REVOKE_NOK;
		
		String rootUrl = Global.get().getNextUrl();
		if (!Global.get().isAuthed()) {
			if (!auth(SysConfig.get().getMqUserId(), SysConfig.get().getMqUserPwd())) {
				return ret;
			}
		}
		
		String reqUrl = String.format("%s/%s/%s", rootUrl, CONSTS.CONFIGSVR, CONSTS.FUN_PURGE_QUEUE);
		String reqParam = String.format("%s=%s&%s=%s&%s=%s", CONSTS.PARAM_NAME, name,
				CONSTS.JSON_HEADER_QUEUE_TYPE, String.valueOf(type),
				CONSTS.PARAM_MAGIC_KEY, Global.get().getMagicKey());
		
		SVarObject sVarInvoke = new SVarObject();
		boolean retInvoke = HttpUtils.postData(reqUrl, reqParam, sVarInvoke);
		if (retInvoke) {
			JSONObject jsonObj = JSONObject.parseObject(sVarInvoke.getVal());
			ret = jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE);
			
			if (ret != CONSTS.REVOKE_OK) {
				String retInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
				sVar.setVal(retInfo);
				
				if (ret == CONSTS.REVOKE_AUTH_FAIL) {
					Global.get().clearAuth();
					Global.get().setLastError(CONSTS.ERR_AUTH_FAIL);
				} else {
					String errInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
					Global.get().setLastError(errInfo);
				}
			}
		} else {
			String err = String.format("http request:%s error.", reqUrl);
			sVar.setVal(err);
			
			logger.error(Global.get().getLastError());
			Global.get().putBrokenUrl(rootUrl);
		}
		
		return ret;
	}
	
	public static int checkUrl(String url) {
		int ret = CONSTS.REVOKE_NOK;
		
		String reqUrl = String.format("%s/%s/%s", url, CONSTS.CONFIGSVR, CONSTS.FUN_URL_TEST);
		
		SVarObject sVarInvoke = new SVarObject();
		boolean retInvoke = HttpUtils.getData(reqUrl, sVarInvoke);
		if (retInvoke) {
			JSONObject jsonObj = JSONObject.parseObject(sVarInvoke.getVal());
			ret = jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE);
		} else {
			logger.error("http request:{} error.", reqUrl);
			logger.error(Global.get().getLastError());
		}
		
		return ret;
	}
	
	public static int getAllQueuesByGroupID(String groupID, SVarObject sVar) {
		int ret = CONSTS.REVOKE_NOK;
		
		String rootUrl = Global.get().getNextUrl();
		if (!Global.get().isAuthed()) {
			if (!auth(SysConfig.get().getMqUserId(), SysConfig.get().getMqUserPwd())) {
				return ret;
			}
		}
		
		String reqUrl = String.format("%s/%s/%s", rootUrl, CONSTS.CONFIGSVR, "getAllQueuesByGroupID");
		String reqParam = String.format("%s=%s&%s=%s",
				CONSTS.JSON_HEADER_GROUP_ID, groupID,
				CONSTS.PARAM_MAGIC_KEY, Global.get().getMagicKey());
		
		SVarObject sVarInvoke = new SVarObject();
		boolean retInvoke = HttpUtils.postData(reqUrl, reqParam, sVarInvoke);
		if (retInvoke) {
			JSONObject jsonObj = JSONObject.parseObject(sVarInvoke.getVal());
			ret = jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE);
			
			String retInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
			sVar.setVal(retInfo);
			
			if (ret != CONSTS.REVOKE_OK) {
				if (ret == CONSTS.REVOKE_AUTH_FAIL) {
					Global.get().clearAuth();
					Global.get().setLastError(CONSTS.ERR_AUTH_FAIL);
				} else {
					String errInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
					Global.get().setLastError(errInfo);
				}
			}
		} else {
			String err = String.format("http request:%s error.", reqUrl);
			sVar.setVal(err);
			
			logger.error(Global.get().getLastError());
			Global.get().putBrokenUrl(rootUrl);
		}
		
		return ret;
	}
	
	public static int getAllVBrokerByGroupID(String groupID, SVarObject sVar) {
		int ret = CONSTS.REVOKE_NOK;
		
		String rootUrl = Global.get().getNextUrl();
		if (!Global.get().isAuthed()) {
			if (!auth(SysConfig.get().getMqUserId(), SysConfig.get().getMqUserPwd())) {
				return ret;
			}
		}
		
		String reqUrl = String.format("%s/%s/%s", rootUrl, CONSTS.CONFIGSVR, "getAllVBrokerByGroupID");
		String reqParam = String.format("%s=%s&%s=%s",
				CONSTS.JSON_HEADER_GROUP_ID, groupID,
				CONSTS.PARAM_MAGIC_KEY, Global.get().getMagicKey());
		
		SVarObject sVarInvoke = new SVarObject();
		boolean retInvoke = HttpUtils.postData(reqUrl, reqParam, sVarInvoke);
		if (retInvoke) {
			JSONObject jsonObj = JSONObject.parseObject(sVarInvoke.getVal());
			ret = jsonObj.getIntValue(CONSTS.JSON_HEADER_RET_CODE);
			
			String retInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
			sVar.setVal(retInfo);
			
			if (ret != CONSTS.REVOKE_OK) {
				if (ret == CONSTS.REVOKE_AUTH_FAIL) {
					Global.get().clearAuth();
					Global.get().setLastError(CONSTS.ERR_AUTH_FAIL);
				} else {
					String errInfo = jsonObj.getString(CONSTS.JSON_HEADER_RET_INFO);
					Global.get().setLastError(errInfo);
				}
			}
		} else {
			String err = String.format("http request:%s error.", reqUrl);
			sVar.setVal(err);
			
			logger.error(Global.get().getLastError());
			Global.get().putBrokenUrl(rootUrl);
		}
		
		return ret;
	}

}
