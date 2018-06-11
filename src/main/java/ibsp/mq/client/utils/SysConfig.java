package ibsp.mq.client.utils;

public class SysConfig {

	private static SysConfig config = null;
	private static Object mtx = null;
	
	static {
		mtx = new Object();
	}

	public static SysConfig get() {
		synchronized (mtx) {
			if (SysConfig.config == null) {
				SysConfig.config = new SysConfig();
			}
		}

		return SysConfig.config;
	}

	private String mq_type = "rabbitmq";
	private String mq_configsvr_rooturl = "http://127.0.0.1:9991/configsvr";
	private boolean mq_zklocker_support = false;
	private String mq_zookeeper_rooturl = "192.168.14.205:2181";
	private boolean debug = false;
	private boolean mq_pub_confirm = false;
	private int mq_prefetch_size = CONSTS.PREFETCH_COUNT;
	private int mq_router_multiplexing_ratio = CONSTS.MULTIPLEXING_RATIO;
	private long mq_write_timeout = CONSTS.WRITE_TIMEOUT;
	private String mq_userid = "";
	private String mq_userpwd = "";

	public SysConfig() {
		this.mq_type = PropertiesUtils.getInstance(CONSTS.MQ_PROP_FILE).get(CONSTS.MQ_CONF_TYPE);
		this.mq_configsvr_rooturl = PropertiesUtils.getInstance(CONSTS.MQ_PROP_FILE).get(CONSTS.MQ_CONF_ROOTURL);
		this.mq_zklocker_support  = PropertiesUtils.getInstance(CONSTS.MQ_PROP_FILE).getBoolean(CONSTS.MQ_CONF_ZKLOKER_SUPPORT, false);
		this.mq_zookeeper_rooturl = PropertiesUtils.getInstance(CONSTS.MQ_PROP_FILE).get(CONSTS.MQ_CONF_ZK);
		this.mq_pub_confirm = PropertiesUtils.getInstance(CONSTS.MQ_PROP_FILE).getBoolean(CONSTS.MQ_CONF_PUBCONFIRM, false);
		this.mq_prefetch_size = PropertiesUtils.getInstance(CONSTS.MQ_PROP_FILE).getInt(CONSTS.MQ_CONF_PRETETCHSIZE, CONSTS.PREFETCH_COUNT);
		this.mq_router_multiplexing_ratio = PropertiesUtils.getInstance(CONSTS.MQ_PROP_FILE).getInt(CONSTS.MQ_CONF_MULTIPLEXING_RATIO, CONSTS.MULTIPLEXING_RATIO);
		this.mq_write_timeout = PropertiesUtils.getInstance(CONSTS.MQ_PROP_FILE).getLong(CONSTS.MQ_CONF_WRITE_TIMEOUT, CONSTS.WRITE_TIMEOUT);
		this.debug = PropertiesUtils.getInstance(CONSTS.MQ_PROP_FILE).getBoolean(CONSTS.MQ_DEBUG, false);
		this.mq_userid = PropertiesUtils.getInstance(CONSTS.MQ_PROP_FILE).get(CONSTS.MQ_USERID, "");
		this.mq_userpwd = PropertiesUtils.getInstance(CONSTS.MQ_PROP_FILE).get(CONSTS.MQ_USERPWD, "");
	}

	public String getMqConfRootUrl() {
		return mq_configsvr_rooturl;
	}

	public void setMqConfRootUrl(String mq_configsvr_rooturl) {
		this.mq_configsvr_rooturl = mq_configsvr_rooturl;
	}
	
	public String getMqZKRootUrl() {
		return mq_zookeeper_rooturl;
	}

	public void setMqZKRootUrl(String mq_zookeeper_rooturl) {
		this.mq_zookeeper_rooturl = mq_zookeeper_rooturl;
	}

	public String getMqType() {
		return mq_type;
	}

	public void setMqType(String mq_type) {
		this.mq_type = mq_type;
	}

	public boolean isPubConfirm() {
		return mq_pub_confirm;
	}

	public void setPubConfirm(boolean mq_pub_confirm) {
		this.mq_pub_confirm = mq_pub_confirm;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public String getMqUserId() {
		return mq_userid;
	}

	public void setMqUserId(String mq_userid) {
		this.mq_userid = mq_userid;
	}

	public String getMqUserPwd() {
		return mq_userpwd;
	}

	public void setMqUserPwd(String mq_userpwd) {
		this.mq_userpwd = mq_userpwd;
	}

	public int getMqPrefetchSize() {
		return mq_prefetch_size;
	}

	public void setMqPrefetchSize(int mq_prefetch_size) {
		this.mq_prefetch_size = mq_prefetch_size;
	}

	public int getMqMultiplexingRatio() {
		return mq_router_multiplexing_ratio;
	}

	public void setMqMultiplexingRatio(int mq_router_multiplexing_ratio) {
		this.mq_router_multiplexing_ratio = mq_router_multiplexing_ratio;
	}

	public boolean isMqZKLockerSupport() {
		return mq_zklocker_support;
	}

	public void setMqZKLockerSupport(boolean mq_zklocker_support) {
		this.mq_zklocker_support = mq_zklocker_support;
	}

	public long getMqWriteTimeout() {
		return mq_write_timeout;
	}

	public void setMqWriteTimeout(long mq_write_timeout) {
		this.mq_write_timeout = mq_write_timeout;
	}
	
}
