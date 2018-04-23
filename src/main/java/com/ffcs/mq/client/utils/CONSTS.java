package com.ffcs.mq.client.utils;

public class CONSTS {

	public static final boolean NOT_CLUSTER_TYPE = false;
	public static final boolean CLUSTER_TYPE = true;

	public static final String COMMA = ",";
	public static final String COLON = ":";
	public static final String VLINE = "|";
	public static final String SBRACKET_LEFT = "[";
	public static final String SBRACKET_RIGHT = "]";
	
	public static final int RETRY_CNT = 5;
	public static final int RETRY_INTERVAL = 20;
	
	public static final int GET_IP_RETRY = 5;
	public static final int GET_IP_RETRY_INTERVAL = 500;
	public static final int LSNR_INVALID_PORT = -1;
	
	public static final int REVOKE_GETDATA = 1;
	public static final int REVOKE_OK = 0;
	public static final int REVOKE_NOK = -1;
	public static final int REVOKE_NOK_QUEUE_EXIST = -2;
	public static final int REVOKE_AUTH_FAIL = -3;
	public static final int REVOKE_NOK_NET_EXCEPTION = -4;
	public static final int REVOKE_NOK_SHUNDOWN = -5;

	public static final int PREFETCH_COUNT = 100;
	public static final boolean PREFETCH_GLOBAL = true;
	public static final int MULTIPLEXING_RATIO = 5;
	public static final long WRITE_TIMEOUT = 3000L;

	public static final long REPLY_TIMEOUT = 3000L;
	public static final long SLEEP_WHEN_NODATA = 1L;
	public static final int CONSUME_BATCH_SLEEP_CNT = 800;

	public static final int BASE_PORT = 9500;
	public static final int BATCH_FIND_CNT = 1000;
	
	public static final int CHANNEL_INVALID = 0;
	public static final int CHANNEL_CMD = 1;
	public static final int CHANNEL_SEND = 2;
	public static final int CHANNEL_REV_START = 3;

	public static final int DELIVERY_MODE_NO_DURABLE = 1;
	public static final int DELIVERY_MODE_DURABLE = 2;

	public static final int RECONNECT_INTERVAL     = 1000;  // 重连间隔
	public static final int STATISTIC_INTERVAL     = 1000;  // 统计间隔
	public static final int REPORT_INTERVAL        = 10000; // 定时上报间隔
	public static final int EVENT_DISPACH_INTERVAL = 10;    // 事件派发空闲休眠间隔

	public static final int FIX_HEAD_LEN    = 10;
	public static final int FIX_PREHEAD_LEN = 6;
	public static final byte[] PRE_HEAD     = {'$','H','E','A','D',':'};
	
	public static final String PRODUCER = "producer";
	public static final String CONSUMER = "consumer";
	public static final int TYPE_QUEUE = 1;
	public static final int TYPE_TOPIC = 2;
	public static final String DURABLE = "1";
	public static final String NOT_DURABLE = "0";
	public static final String ORDERED = "1";
	public static final String NOT_ORDERED = "0";
	public static final String NOT_GLOBAL_ORDERED = "0";
	public static final String GLOBAL_ORDERED = "1";
	public static final String DEPLOYED = "1";
	public static final String NOT_DEPLOYED = "0";
	public static final String CLUSTER = "1";
	public static final String NOT_CLUSTER = "0";
	public static final String NOT_WRITABLE = "0";
	public static final String WRITABLE     = "1";

	public static final int TOPIC_DEFAULT = 0;
	public static final int TOPIC_ANONYMOUS = 1;
	public static final int TOPIC_PERMERNENT = 2;
	public static final int TOPIC_WILDCARD = 3;

	public static final int TYPE_NULL = 0; // default
	public static final int TYPE_PRO = 1; // producer type
	public static final int TYPE_CON = 2; // consumer type
	public static final int TYPE_MIX = 3; // multitype: producer and consumer
	
	public static final String ZK_LOCK_ROOTPATH = "/ffcs_mq/lock";
	public static final int ZK_SESSION_TIMEOUT = 5000;
	public static final int ZK_CONN_TIMEOUT = 3000;
	public static final int ZK_CONN_RETRY = 10000;
	public static final int ZK_CONN_RETRY_INTERVAL = 1000;

	public static final boolean ACK_MULTIPLE = false;
	public static final boolean AUTO_ACK = false;

	public static final String AMQ_DIRECT = "amq.direct";
	public static final String GEN_TAG_PREFIX = "amq.ctag-gentag";
	public static final String PERM_CON_PREFIX = "CON_ID";
	public static final boolean AMQ_MANDATORY = true;
	public static final boolean AMQ_IMMEDIATE = false;

	public static final String CONF_PATH = "conf";
	public static final String MQ_PROP_FILE = "mq";
	public static final String MQ_CONF_TYPE = "mq.type";
	public static final String MQ_CONF_ROOTURL = "mq.configsvr.rooturl";
	public static final String MQ_CONF_ZKLOKER_SUPPORT = "mq.zklocker.support";
	public static final String MQ_CONF_ZK = "mq.zookeeper.rooturl";
	public static final String MQ_CONF_PUBCONFIRM = "mq.publish.confirm";
	public static final String MQ_CONF_PRETETCHSIZE = "mq.prefetch.size";
	public static final String MQ_CONF_MULTIPLEXING_RATIO = "mq.router.multiplexing.ratio";
	public static final String MQ_CONF_WRITE_TIMEOUT = "mq.write.timeout";
	public static final String MQ_TYPE_RABBITMQ = "rabbitmq";
	public static final String DEBUG = "debug";
	public static final String MQ_USERID  = "mq.userid";
	public static final String MQ_USERPWD = "mq.userpwd";

	public static final String HTTP_METHOD_GET = "GET";
	public static final String HTTP_METHOD_POST = "POST";

	public static final String HTTP_PROTOCAL = "http";
	public static final String CONFIGSVR = "configsvr";
	public static final String METASVR = "metasvr";
	public static final String MQSVR = "mqsvr";

	public static final String FUN_GETQUEUEBYQNAME = "getQueueByName";
	public static final String FUN_GETBORKERSBYQNAME = "getBrokersByQName";
	public static final String FUN_CREATEQUEUEBYCLIENT = "createQueueByClient";
	public static final String FUN_DELETEQUEUEBYCLIENT = "deleteQueueByClient";
	public static final String FUN_PUT_CLNT_STAT_INFO = "putClientStatisticInfo";
	public static final String FUN_URL_MESSAGE_READY = "getMessageReady";
	public static final String FUN_GEN_CONSUMER_ID = "genConsumerID";
	public static final String FUN_GETPERMNENTTOPIC = "getPermnentTopic";
	public static final String FUN_PUTPERMNENTTOPIC = "savePermnentTopic";
	public static final String FUN_DELPERMNENTTOPIC = "delPermnentTopic";
	public static final String FUN_PURGE_QUEUE = "purgeQueueByClient";
	public static final String FUN_URL_TEST = "test";
	public static final String FUN_URL_AUTH = "auth";
	
	public static final String PARAM_NAME  = "name";
	public static final String PARAM_QNAME = "qname";
	public static final String PARAM_USER_ID = "USER_ID";
	public static final String PARAM_USER_PWD = "USER_PWD";
	public static final String PARAM_QUEUENAME = "QUEUE_NAME";
	public static final String PARAM_QUEUETYPE = "QUEUE_TYPE";
	public static final String PARAM_DURABLE = "IS_DURABLE";
	public static final String PARAM_ORDERED = "GLOBAL_ORDERED";
	public static final String PARAM_GROUPID = "GROUP_ID";
	public static final String PARAM_CLIENTINFO = "CLIENT_INFO";
	public static final String PARAM_LSNRADDR = "LSNR_ADDR";
	public static final String PARAM_MAGIC_KEY = "MAGIC_KEY";

	public static final String JSON_HEADER_ID = "ID";
	public static final String JSON_HEADER_NAME = "NAME";
	public static final String JSON_HEADER_BROKERS = "BROKERS";
	public static final String JSON_HEADER_BROKERID = "BROKER_ID";
	public static final String JSON_HEADER_BROKERNAME = "BROKER_NAME";
	public static final String JSON_HEADER_VBROKERID = "VBROKER_ID";
	public static final String JSON_HEADER_VBROKERNAME = "VBROKER_NAME";
	public static final String JSON_HEADER_HOSTNAME = "HOSTNAME";
	public static final String JSON_HEADER_IP = "IP";
	public static final String JSON_HEADER_VIP = "VIP";
	public static final String JSON_HEADER_PORT = "PORT";
	public static final String JSON_HEADER_MGRPORT = "MGR_PORT";
	public static final String JSON_HEADER_USER = "MQ_USER";
	public static final String JSON_HEADER_PASSWORD = "MQ_PWD";
	public static final String JSON_HEADER_VHOST = "VHOST";
	public static final String JSON_HEADER_MASTER_ID = "MASTER_ID";
	public static final String JSON_HEADER_ERL_COOKIE = "ERL_COOKIE";
	public static final String JSON_HEADER_CLUSTER = "IS_CLUSTER";
	public static final String JSON_HEADER_WRITABLE = "IS_WRITABLE";
	public static final String JSON_HEADER_QUEUE_ID = "QUEUE_ID";
	public static final String JSON_HEADER_QUEUE_NAME = "QUEUE_NAME";
	public static final String JSON_HEADER_QUEUE_TYPE = "QUEUE_TYPE";
	public static final String JSON_HEADER_IS_DURABLE = "IS_DURABLE";
	public static final String JSON_HEADER_IS_ORDERED = "IS_ORDERED";
	public static final String JSON_HEADER_IS_DEPLOY = "IS_DEPLOY";
	public static final String JSON_HEADER_MAIN_TOPIC = "MAIN_TOPIC";
	public static final String JSON_HEADER_SUB_TOPIC = "SUB_TOPIC";
	public static final String JSON_HEADER_GROUP_ID = "GROUP_ID";
	public static final String JSON_HEADER_GROUP_NAME = "GROUP_NAME";
	public static final String JSON_HEADER_SERV_ID = "SERV_ID";
	public static final String JSON_HEADER_SERV_NAME = "SERV_NAME";
	public static final String JSON_HEADER_CONSUMER_ID = "CONSUMER_ID";
	public static final String JSON_HEADER_PERM_QUEUE = "PERM_QUEUE";
	public static final String JSON_HEADER_SRC_QUEUE = "SRC_QUEUE";
	public static final String JSON_HEADER_REAL_QUEUE = "REAL_QUEUE";
	public static final String JSON_HEADER_RET_CODE = "RET_CODE";
	public static final String JSON_HEADER_RET_INFO = "RET_INFO";
	public static final String JSON_HEADER_CLIENT_TYPE = "CLIENT_TYPE";
	public static final String JSON_HEADER_CLNT_IP_PORT = "CLIENT_IP_AND_PORT";
	public static final String JSON_HEADER_BKR_IP_PORT = "BROKER_IP_AND_PORT";
	public static final String JSON_HEADER_CLNT_PRO_TPS = "CLIENT_PRO_TPS";
	public static final String JSON_HEADER_CLNT_CON_TPS = "CLIENT_CON_TPS";
	public static final String JSON_HEADER_T_PRO_MSG_COUNT = "TOTAL_PRO_MSG_COUNT";
	public static final String JSON_HEADER_T_PRO_MSG_BYTES = "TOTAL_PRO_MSG_BYTES";
	public static final String JSON_HEADER_T_CON_MSG_COUNT = "TOTAL_CON_MSG_COUNT";
	public static final String JSON_HEADER_T_CON_MSG_BYTES = "TOTAL_CON_MSG_BYTES";
	public static final String JSON_HEADER_LOCAL_IP = "LOCAL_IP";
	public static final String JSON_HEADER_LOCAL_PORT = "LOCAL_PORT";
	public static final String JSON_HEADER_REMOTE_IP = "REMOTE_IP";
	public static final String JSON_HEADER_REMOTE_PORT = "REMOTE_PORT";
	public static final String JSON_HEADER_MSG_READY = "messages_ready";
	public static final String JSON_HEADER_MAGIC_KEY = "MAGIC_KEY";

	public static final String EV_CODE = "EVENT_CODE";
	public static final String EV_QUEUE_ID = "QUEUE_ID";
	public static final String EV_QUEUE_NAME = "QUEUE_NAME";
	public static final String EV_GROUP_ID = "GROUP_ID";
	public static final String EV_VBROKER_ID = "VBROKER_ID";
	public static final String EV_BROKER_ID = "BROKER_ID";
	public static final String EV_JSONSTR = "JSON_STR";

	public static final String ERR_NOT_ALL_NODES_RDY = "not all nodes ready!";
	public static final String ERR_NO_VALID_NODES    = "no valid nodes!";
	public static final String ERR_NO_VALID_WIRTABLE_NODES = "no valid witable nodes!";
	public static final String ERR_NO_QUEUE_LISTENED = "no queue is listened!";
	public static final String ERR_AUTH_FAIL = "auth fail!";

	public static final String MQ_DEFAULT_USER       = "mq";
	public static final String MQ_DEFAULT_PWD        = "ibsp_mq@123321";
	public static final String MQ_DEFAULT_VHOST      = "/";
}
