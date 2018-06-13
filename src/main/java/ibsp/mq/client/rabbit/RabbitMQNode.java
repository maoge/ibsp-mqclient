package ibsp.mq.client.rabbit;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibsp.common.utils.BlockingLock;
import ibsp.common.utils.CONSTS;
import ibsp.common.utils.IBSPConfig;
import ibsp.common.utils.SRandomGenerator;
import ibsp.common.utils.SVarObject;
import ibsp.common.utils.StringUtils;
import ibsp.mq.client.api.MQMessage;
import ibsp.mq.client.bean.QueueDtlBean;
import ibsp.mq.client.router.Broker;
import ibsp.mq.client.router.IMQNode;
import ibsp.mq.client.router.VBroker;
import ibsp.mq.client.utils.Global;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class RabbitMQNode implements IMQNode {

	private static Logger logger = LoggerFactory.getLogger(RabbitMQNode.class);

	private VBroker vbroker;
	private Connection conn;
	private Channel cmdChannel; // 最开始为固定的一个连接2个Channel: 1-cmdChannel
								// 2-dataChannel,
								// 后面提出指定队列名来消费的需求发现同时监听多个队列公用同一个dataChannel
								// 存在缓冲被其中一个队列的数据塞满其它对列无法取到的问题
								// 现调整为 1-cmdChannel 2-sendChannel(发送用)
								// 从3开始为消费者使用的channel
	private Channel sendChannel;
	private RabbitRetListener sendRetLsnr;
	private RabbitConfirmListener sendConfirmLsnr;
	private volatile BlockingLock<Integer> retLock;
	
	private Set<Integer> usedChannel;
	private Map<String, QueueDtlBean> queueDtlMap;
	private Map<String, QueueDtlBean> queueDtlBackupMap;

	private BasicProperties sendProperties;
	private BasicProperties.Builder sendBuilder;

	private String localAddr; // ip:port

	boolean isLsnr = false;
	boolean isCmdChInited = false;
	boolean isSendChInited = false;
	
	private static boolean isPubConfig = IBSPConfig.getInstance().MqIsPubConfirm();

	public RabbitMQNode() {
		usedChannel = new HashSet<Integer>();
		this.queueDtlMap = new HashMap<String, QueueDtlBean>();
		this.queueDtlBackupMap = new HashMap<String, QueueDtlBean>();
		sendBuilder = new BasicProperties.Builder();
	}
	
	public RabbitMQNode(Map<String, QueueDtlBean> queueDtlMap, VBroker vbroker) {
		this.vbroker = vbroker;
		
		usedChannel = new HashSet<Integer>();
		this.queueDtlMap = new HashMap<String, QueueDtlBean>();
		this.queueDtlBackupMap = queueDtlMap != null ? queueDtlMap : new HashMap<String, QueueDtlBean>();
		sendBuilder = new BasicProperties.Builder();
	}

	public RabbitMQNode(VBroker vbroker) {
		this.vbroker = vbroker;
		this.usedChannel = new HashSet<Integer>();
		this.queueDtlMap = new HashMap<String, QueueDtlBean>();

		sendBuilder = new BasicProperties.Builder();
	}

	@Override
	public void setVBrokerInfo(VBroker vbroker) {
		this.vbroker = vbroker;
	}

	@Override
	public VBroker getVBrokerInfo() {
		return vbroker;
	}
	
	private int getValidChannel() {
		int ret = CONSTS.CHANNEL_INVALID;
		int end = CONSTS.CHANNEL_REV_START + usedChannel.size() + 1;
		for (int i = CONSTS.CHANNEL_REV_START; i < end; i++)
		{
			if (!usedChannel.contains(i))
			{
				ret = i;
				break;
			}
		}
		return ret;
	}

	@Override
	public int connect() {
		int res = CONSTS.REVOKE_NOK;

		ConnectionFactory factory = new ConnectionFactory();

		if (vbroker == null) {
			Global.get().setLastError("broker info is null, need setBrokerInfo or check the broker");
			return res;
		}
		
		Broker broker = vbroker.getMasterBroker();
		factory.setUsername(broker.getMqUser());
		factory.setPassword(broker.getMqPwd());
		factory.setVirtualHost(broker.getVHost());
		factory.setHost(broker.getIP());
		factory.setPort(broker.getPort());

		try {
			conn = factory.newConnection();

			if (conn != null) {
				Socket sock = factory.getSocket();
				if (sock != null && sock.isConnected()) {
					localAddr = sock.getLocalAddress().getHostAddress() + CONSTS.COLON + sock.getLocalPort();
					res = CONSTS.REVOKE_OK;
				}
			}
			
			String info = String.format("Reconnect %s:%d success!", broker.getIP(), broker.getPort());
			logger.info(info);
		} catch (Exception e) {
			// IOException ConnectException TimeoutException
			String err = String.format("Broker [brokerId=%s, ip=%s, port=%d] connect fail, %s",
					broker.getBrokerId(), broker.getIP(), broker.getPort(), e.getMessage());
			logger.error(err, e);
			Global.get().setLastError(e.getMessage());
		}

		return res;
	}

	private boolean initCmdChannel() {
		if (isCmdChInited)
			return true;

		boolean ret = false;

		if (conn != null && conn.isOpen()) {
			try {
				cmdChannel = conn.createChannel(CONSTS.CHANNEL_CMD);

				isCmdChInited = true;
				ret = true;
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				
				Broker broker = vbroker.getMasterBroker();
				Global.get().setLastError(broker.getIP() + ":" + broker.getPort() + " initCmdChannel error:" + e.getMessage());
			}
		}

		return ret;
	}
	
	private void closeCmdChannel() {
		if (!isCmdChInited)
			return;
		
		if (isCmdChInited && cmdChannel != null) {
			try {
				if (cmdChannel.isOpen())
					cmdChannel.close();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			} finally {
				cmdChannel = null;
				isCmdChInited = false;
			}
		}
	}

	private boolean initSendChannel() {
		if (isSendChInited)
			return true;

		boolean ret = false;

		if (conn != null && conn.isOpen()) {
			try {
				sendChannel = conn.createChannel(CONSTS.CHANNEL_SEND);

				if (isPubConfig) {
					sendChannel.confirmSelect();

					retLock = new BlockingLock<Integer>();
					sendRetLsnr = new RabbitRetListener(retLock);
					sendChannel.addReturnListener(sendRetLsnr);

					// 回调顺序 先ReturnListener 再ConfirmListener
					// basicSend和basicPublish正常时 ReturnListener没有回调,
					// ConfirmListener一定回调, 防止调用的地方长时间等待retLock
					// 利用 ConfirmListener 的回调触发retLock.set, 从而触发调用的地方wakeup
					sendConfirmLsnr = new RabbitConfirmListener(retLock);
					sendChannel.addConfirmListener(sendConfirmLsnr);
				}

				isSendChInited = true;
				ret = true;
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				Global.get().setLastError(e.getMessage());
			}
		}

		return ret;
	}

	@Override
	public int close() {
		int res = CONSTS.REVOKE_NOK;

		try {
			Set<Entry<String, QueueDtlBean>> entrySet = queueDtlMap.entrySet();
			for (Entry<String, QueueDtlBean> entry : entrySet) {
				QueueDtlBean queueTypeBean = entry.getValue();
				if (queueTypeBean == null)
					continue;

				String consumerTag = queueTypeBean.getConsumerTag();
				Channel dataChannel = queueTypeBean.getDataChannel();
				if (dataChannel != null && dataChannel.isOpen()) {
					try {
						dataChannel.basicCancel(consumerTag);
						dataChannel.close();
					} catch (Exception e) {
					} finally {
						dataChannel = null;
					}
				}
			}

			if (isCmdChInited && cmdChannel != null) {
				try {
					if (cmdChannel.isOpen())
						cmdChannel.close();
				} catch (Exception e) {
				} finally {
					cmdChannel = null;
					isCmdChInited = false;
				}
			}

			if (isSendChInited && sendChannel != null) {
				try {
					if (sendChannel.isOpen())
						sendChannel.close();
				} catch (Exception e) {
				} finally {
					sendChannel = null;
					isSendChInited = false;
				}
			}

			if (conn != null) {
				try {
					if (conn.isOpen())
						conn.close();
				} catch (Exception e) {
				} finally {
					conn = null;
				}
			}

			queueDtlMap.clear();
			usedChannel.clear();
			isLsnr = false;
			localAddr = "";

			res = CONSTS.REVOKE_OK;

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		}

		return res;
	}

	@Override
	public int softcut() {
		int res = CONSTS.REVOKE_NOK;

		try {
			Set<Entry<String, QueueDtlBean>> entrySet = queueDtlMap.entrySet();
			for (Entry<String, QueueDtlBean> entry : entrySet) {
				String key = entry.getKey();
				QueueDtlBean queueTypeBean = entry.getValue();
				if (queueTypeBean == null)
					continue;

				String consumerTag = queueTypeBean.getConsumerTag();
				Channel dataChannel = queueTypeBean.getDataChannel();
				if (dataChannel != null && dataChannel.isOpen()) {
					try {
						dataChannel.basicCancel(consumerTag);
						dataChannel.close();
					} catch (Exception e) {
					} finally {
						dataChannel = null;
					}
				}

				// 保存下来重连成功后方便重新监听
				queueDtlBackupMap.put(key, queueTypeBean);
			}

			if (isCmdChInited && cmdChannel != null) {
				try {
					if (cmdChannel.isOpen())
						cmdChannel.close();
				} catch (Exception e) {
				} finally {
					cmdChannel = null;
					isCmdChInited = false;
				}
			}

			if (isSendChInited && sendChannel != null) {
				try {
					if (sendChannel.isOpen())
						sendChannel.close();
				} catch (Exception e) {
				} finally {
					sendChannel = null;
					isSendChInited = false;
				}
			}

			if (conn != null) {
				try {
					if (conn.isOpen())
						conn.close();
				} catch (Exception e) {
				} finally {
					conn = null;
				}
			}

			queueDtlMap.clear();
			usedChannel.clear();
			localAddr = "";
			res = CONSTS.REVOKE_OK;

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		}

		return res;
	}
	
	@Override
	public int forceClose() throws IOException {
		sendChannel.forceClose();
		return CONSTS.REVOKE_OK;
	}

	@Override
	public int relistenAfterBroken() {
		boolean withError = false;
		
		Set<String> success = new HashSet<String>();

		Set<Entry<String, QueueDtlBean>> entrySet = queueDtlBackupMap.entrySet();
		for (Entry<String, QueueDtlBean> entry : entrySet) {
			String name = entry.getKey();
			QueueDtlBean qDtlBean = entry.getValue();

			if (qDtlBean == null)
				continue;

			int res = CONSTS.REVOKE_NOK;
			if (qDtlBean.getType() == CONSTS.TYPE_QUEUE && qDtlBean.getTopicType() == CONSTS.TOPIC_DEFAULT) {
				res = listenQueue(name);
			} else if (qDtlBean.getType() == CONSTS.TYPE_TOPIC && qDtlBean.getTopicType() == CONSTS.TOPIC_ANONYMOUS) {
				res = listenTopicAnonymous(name);
			} else if (qDtlBean.getType() == CONSTS.TYPE_TOPIC && qDtlBean.getTopicType() == CONSTS.TOPIC_PERMERNENT) {
				String realQueueName = qDtlBean.getRealQueueName();
				String conumserId = qDtlBean.getConsumerId();
				res = listenTopicPermnent(name, realQueueName, conumserId);
			}

			if (res == CONSTS.REVOKE_NOK) {
				logger.error("relisten:" + name + " fail.");
				withError = true;
				break;
			} else {
				success.add(name);
			}
		}
		
		for (String s : success) {
			queueDtlBackupMap.remove(s);
		}

		if (!withError) {
			logger.info("relistenAfterBroken all ok.");
		} else {
			logger.info("relistenAfterBroken not all ok.");
		}

		return withError ? CONSTS.REVOKE_NOK : CONSTS.REVOKE_OK;
	}

	@Override
	public boolean isConnect() {
		if (conn == null) {
			Broker broker = vbroker.getMasterBroker();
			Global.get().setLastError(broker.getIP() + ":" + broker.getPort() + " connection not inited.");
			return false;
		}
		
		boolean ret = false;
		try {
			ret = conn.isOpen();
		} catch (Exception e) {
		}
		return ret;
	}

	public boolean isListener() {
		return isLsnr;
	}

	/**
	 * 
	 * @param queueName
	 * @param durable
	 * @param autoDelete
	 * @param exclusive
	 * @return 0:SUCCESS -1:FAIL
	 */
	public int queueDeclare(String queue, boolean durable, boolean autoDelete, boolean exclusive, Map<String, Object> arguments) {
		int res = CONSTS.REVOKE_NOK;

		try {
			if (isConnect()) {
				if (!isCmdChInited) {
					if (!initCmdChannel()) {
						return CONSTS.REVOKE_NOK;
					}
				}

				Queue.DeclareOk decResult = cmdChannel.queueDeclare(queue, durable, exclusive, autoDelete, arguments);

				if (decResult != null && decResult.getQueue().equals(queue)) {
					res = CONSTS.REVOKE_OK;
				}
			}

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		} finally {
			closeCmdChannel();
		}

		return res;
	}

	/**
	 * create anonyous queue
	 * 
	 * @param sObj
	 *            passout anonyous queue name
	 * @param durable
	 * @param autoDelete
	 * @param arguments
	 * @return 0:SUCCESS -1:FAIL
	 */
	public int queueDeclareAnonymous(SVarObject sObj, Map<String, Object> arguments) {
		int res = CONSTS.REVOKE_NOK;

		try {
			if (isConnect()) {
				if (!isCmdChInited) {
					if (!initCmdChannel()) {
						return CONSTS.REVOKE_NOK;
					}
				}

				Queue.DeclareOk decResult = cmdChannel.queueDeclare("", false, true, true, arguments); // durable
																										// =
																										// false,
																										// exclusive
																										// =
																										// true,
																										// autoDelete
																										// =
																										// true
				if (decResult != null) {
					sObj.setVal(decResult.getQueue());
					res = CONSTS.REVOKE_OK;
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		} finally {
			closeCmdChannel();
		}

		return res;
	}

	public int queueDeclarePermnent(String realQueueName, SVarObject sObj, Map<String, Object> arguments) {
		int res = CONSTS.REVOKE_NOK;

		try {
			if (isConnect()) {
				if (!isCmdChInited) {
					if (!initCmdChannel()) {
						return CONSTS.REVOKE_NOK;
					}
				}

				Queue.DeclareOk decResult = cmdChannel.queueDeclare(realQueueName, true, false, false, arguments); // durable
																													// =
																													// true,
																													// exclusive
																													// =
																													// false,
																													// autoDelete
																													// =
																													// false
				if (decResult != null) {
					sObj.setVal(decResult.getQueue());
					res = CONSTS.REVOKE_OK;
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		} finally {
			closeCmdChannel();
		}

		return res;
	}

	/**
	 * 
	 * @param queueName
	 * @return 0:SUCCESS -1:FAIL
	 */
	public int queueDeclarePassive(String queue) {
		int res = CONSTS.REVOKE_NOK;

		try {
			if (isConnect()) {
				if (!isCmdChInited) {
					if (!initCmdChannel()) {
						return CONSTS.REVOKE_NOK;
					}
				}

				Queue.DeclareOk decResult = cmdChannel.queueDeclarePassive(queue);

				if (decResult != null && decResult.getQueue().equals(queue)) {
					res = CONSTS.REVOKE_OK;
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		} finally {
			closeCmdChannel();
		}

		return res;
	}

	/**
	 * 
	 * @param queueName
	 * @return 0:SUCCESS -1:FAIL
	 */
	public int queueDelete(String queue) {
		int res = CONSTS.REVOKE_NOK;

		try {
			if (isConnect()) {
				if (!isCmdChInited) {
					if (!initCmdChannel()) {
						return CONSTS.REVOKE_NOK;
					}
				}

				Queue.DeleteOk delResult = cmdChannel.queueDelete(queue);

				if (delResult != null) {
					res = CONSTS.REVOKE_OK;
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		} finally {
			closeCmdChannel();
		}

		return res;
	}

	/**
	 * 
	 * @param queueName
	 * @return 0:SUCCESS -1:FAIL
	 */
	public int listenQueue(String queue) {
		int res = CONSTS.REVOKE_NOK;

		if (queueDtlMap.containsKey(queue)) {
			Global.get().setLastError("queue:" + queue + " already listened ......");
			return CONSTS.REVOKE_NOK;
		}

		try {
			if (isConnect()) {
				int channelIdx = getValidChannel();
				Channel dataChannel = conn.createChannel(channelIdx);
				dataChannel.basicQos(Global.get().getQos(), CONSTS.PREFETCH_GLOBAL);

				String localConsumeTag = SRandomGenerator.genConsumerTag();
				QueueingConsumer consumer = new QueueingConsumer(dataChannel);
				String serverConsumerTag = dataChannel.basicConsume(queue, CONSTS.AUTO_ACK, localConsumeTag, consumer);

				if (serverConsumerTag.equals(localConsumeTag)) {
					QueueDtlBean qDtl = new QueueDtlBean(queue, CONSTS.TYPE_QUEUE, CONSTS.TOPIC_DEFAULT, queue, localConsumeTag, consumer,
							dataChannel, channelIdx, "");
					queueDtlMap.put(queue, qDtl);
					usedChannel.add(channelIdx);

					res = CONSTS.REVOKE_OK;
				} else {
					Global.get().setLastError("listenQueue:" + queue + " failed.");
				}
			} else {
				Global.get().setLastError("dataChannel null or closed ......");
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		}

		return res;
	}

	/**
	 * 
	 * @param queue_name
	 * @return 0:SUCCESS -1:FAIL
	 */
	public int unlistenQueue(String queue) {
		int res = CONSTS.REVOKE_NOK;

		try {
			if (!queueDtlMap.containsKey(queue)) {
				Global.get().setLastError("queue:" + queue + " not listened ......");
			} else {
				QueueDtlBean qDtl = queueDtlMap.get(queue);
				if (qDtl.getType() != CONSTS.TYPE_QUEUE) {
					Global.get().setLastError("queue:" + queue + " not listened or listened as topic mode ......");
					return CONSTS.REVOKE_NOK;
				}

				if (isConnect()) {
					String consumerTag = qDtl.getConsumerTag();
					Channel dataChannel = qDtl.getDataChannel();
					int channelIdx = qDtl.getChannelIdx();

					boolean closeOk = false;
					if (dataChannel != null && dataChannel.isOpen()) {
						dataChannel.basicCancel(consumerTag);

						dataChannel.close();
						dataChannel = null;
						closeOk = true;
					}

					if (closeOk) {
						queueDtlMap.remove(queue);
						usedChannel.remove(channelIdx);
						res = CONSTS.REVOKE_OK;
					}
				}
			}

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		}

		return res;
	}

	/**
	 * 匿名方式监听topic, 监听成功会自动创建一个临时队列, 优点连接断开自动释放临时队列, 弊端就是闪断到重新监听这段时间的消息就无法接收到
	 * 
	 * @param queueName
	 * @return 0:SUCCESS -1:FAIL
	 */
	public int listenTopicAnonymous(String topic) {
		int res = CONSTS.REVOKE_NOK;
		boolean needDelete = false;
		String realQueueName = null;

		if (queueDtlMap.containsKey(topic)) {
			Global.get().setLastError("topic:" + topic + " already listened ......");
			return CONSTS.REVOKE_NOK;
		}

		try {
			if (isConnect()) {
				SVarObject sObj = new SVarObject();
				if (queueDeclareAnonymous(sObj, null) != CONSTS.REVOKE_OK) {
					Global.get().setLastError("listenTopicAnonymous:" + topic + " fail to create anonymous queue");
					return res;
				}

				realQueueName = sObj.getVal();

				int channelIdx = getValidChannel();
				Channel dataChannel = conn.createChannel(channelIdx);
				dataChannel.basicQos(Global.get().getQos(), CONSTS.PREFETCH_GLOBAL);

				Queue.BindOk binResult = dataChannel.queueBind(realQueueName, CONSTS.AMQ_DIRECT, topic);
				if (binResult != null) {
					String localConsumeTag = SRandomGenerator.genConsumerTag();
					QueueingConsumer consumer = new QueueingConsumer(dataChannel);
					String serverConsumeTag = dataChannel.basicConsume(realQueueName, CONSTS.AUTO_ACK, localConsumeTag, consumer);

					if (localConsumeTag.equals(serverConsumeTag)) {
						QueueDtlBean qDtl = new QueueDtlBean(topic, CONSTS.TYPE_TOPIC, CONSTS.TOPIC_ANONYMOUS, realQueueName,
								localConsumeTag, consumer, dataChannel, channelIdx, "");
						queueDtlMap.put(topic, qDtl);
						usedChannel.add(channelIdx);

						res = CONSTS.REVOKE_OK;
					} else {
						Global.get().setLastError("listenTopicAnonymous:" + topic + " failed.");
						needDelete = true;
					}
				} else {
					needDelete = true;
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		} finally {
			if (needDelete) {
				queueDelete(realQueueName);
			}
		}

		return res;
	}

	public int unlistenTopicAnonymous(String topic) {
		int res = CONSTS.REVOKE_NOK;

		try {
			if (!queueDtlMap.containsKey(topic)) {
				String err = String.format("topic:%s not listened.", topic);
				Global.get().setLastError(err);
			} else {
				if (isConnect()) {
					QueueDtlBean qDtl = queueDtlMap.get(topic);
					if (qDtl.getType() != CONSTS.TYPE_TOPIC) {
						String err = String.format("topic:%s not listened as topic, maybe as queue.", topic);
						Global.get().setLastError(err);
						return CONSTS.REVOKE_NOK;
					}

					String consumerTag = qDtl.getConsumerTag();
					Channel dataChannel = qDtl.getDataChannel();
					int channelIdx = qDtl.getChannelIdx();
					
					boolean closeOk = false;
					if (dataChannel != null && dataChannel.isOpen()) {
						dataChannel.basicCancel(consumerTag);

						dataChannel.close();
						dataChannel = null;
						closeOk = true;
					}
					
					if (closeOk) {
						queueDtlMap.remove(topic);
						usedChannel.remove(channelIdx);
						res = CONSTS.REVOKE_OK;
					}
				}
			}

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		}

		return res;
	}

	/**
	 * 持久化方式监听topic, 监听成功会自动创建一个匿名队列(持久化), 优点连接断开队列还存在继续转接消息, 等客户端重连上不丢消息,
	 * 弊端管理难度加大
	 * 
	 * @param queueName
	 * 
	 * @return 0:SUCCESS -1:FAIL
	 */
	public int listenTopicPermnent(String topic, String realQueueName, String consumerId) {
		int res = CONSTS.REVOKE_NOK;

		if (queueDtlMap.containsKey(consumerId)) {
			String info = String.format("topic:%s consumerId:%d already listened ......", topic, consumerId);
			logger.info(info);
			return CONSTS.REVOKE_OK;
		}

		try {
			if (isConnect()) {
				int channelIdx = getValidChannel();
				Channel dataChannel = conn.createChannel(channelIdx);
				dataChannel.basicQos(Global.get().getQos(), CONSTS.PREFETCH_GLOBAL);

				String localConsumeTag = SRandomGenerator.genConsumerTag();
				QueueingConsumer consumer = new QueueingConsumer(dataChannel);
				String serverConsumeTag = dataChannel.basicConsume(realQueueName, CONSTS.AUTO_ACK, localConsumeTag, consumer);

				if (serverConsumeTag.equals(localConsumeTag)) {
					QueueDtlBean qDtl = new QueueDtlBean(topic, CONSTS.TYPE_TOPIC, CONSTS.TOPIC_PERMERNENT, realQueueName, localConsumeTag,
							consumer, dataChannel, channelIdx, consumerId);

					queueDtlMap.put(consumerId, qDtl);
					usedChannel.add(channelIdx);
					res = CONSTS.REVOKE_OK;
				} else {
					Global.get().setLastError("listenTopicPermernent:" + topic + " failed.");
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		}
		
		return res;
	}

	public int unlistenTopicPermnent(String topic, String consumerId) {
		int res = CONSTS.REVOKE_NOK;

		try {
			if (!queueDtlMap.containsKey(consumerId)) {
				String err = String.format("topic:%s consumerId:%s not listened.", topic, consumerId);
				Global.get().setLastError(err);
			} else {
				if (isConnect()) {
					QueueDtlBean qDtl = queueDtlMap.get(consumerId);
					if (qDtl.getType() != CONSTS.TYPE_TOPIC) {
						String err = String.format("topic:%s not listened as topic, maybe as queue.", topic);
						Global.get().setLastError(err);
						return CONSTS.REVOKE_NOK;
					}

					String consumerTag = qDtl.getConsumerTag();
					Channel dataChannel = qDtl.getDataChannel();
					int channelIdx = qDtl.getChannelIdx();
					
					boolean closeOk = false;
					if (dataChannel != null && dataChannel.isOpen()) {
						dataChannel.basicCancel(consumerTag);

						dataChannel.close();
						dataChannel = null;
						closeOk = true;
					}
					
					if (closeOk) {
						queueDtlMap.remove(topic);
						usedChannel.remove(channelIdx);
						res = CONSTS.REVOKE_OK;
					}
				}
			}

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		}

		return res;
	}

	public int createAndBind(String topic, String realQueueName) {
		int res = CONSTS.REVOKE_NOK;

		SVarObject sObj = new SVarObject();
		if (queueDeclarePermnent(realQueueName, sObj, null) != CONSTS.REVOKE_OK) {
			Global.get().setLastError("listenTopicPermernent:" + topic + " fail to create permnent queue");
			return CONSTS.REVOKE_NOK;
		}

		boolean needDelete = false;
		try {
			if (isConnect()) {
				if (!isCmdChInited) {
					if (!initCmdChannel()) {
						return CONSTS.REVOKE_NOK;
					}
				}

				Queue.BindOk binResult = cmdChannel.queueBind(realQueueName, CONSTS.AMQ_DIRECT, topic);
				if (binResult != null) {
					res = CONSTS.REVOKE_OK;
				} else {
					needDelete = true;

					String err = String.format("bind %s to %s fail.", realQueueName, topic);
					Global.get().setLastError(err);
				}

				if (needDelete) {
					queueDelete(realQueueName);
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		} finally {
			closeCmdChannel();
		}

		return res;
	}

	public int unBindAndDelete(String topic, String realQueueName) {
		int res = CONSTS.REVOKE_NOK;

		try {
			if (isConnect()) {
				if (!isCmdChInited) {
					if (!initCmdChannel()) {
						return CONSTS.REVOKE_NOK;
					}
				}

				Queue.UnbindOk unbindResult = cmdChannel.queueUnbind(realQueueName, CONSTS.AMQ_DIRECT, topic);
				if (unbindResult != null) {
					res = queueDelete(realQueueName);
				} else {
					String err = String.format("unbind:%s from %s fail.", realQueueName, topic);
					Global.get().setLastError(err);
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		} finally {
			closeCmdChannel();
		}

		return res;
	}

	/**
	 * 
	 * @param queueName
	 * @param message
	 * @return 0:SUCCESS -1:FAIL
	 */
	public int sendQueue(String queueName, MQMessage message, boolean isDurable) {
		int res = CONSTS.REVOKE_NOK;

		if (queueName == null) {
			Global.get().setLastError("queue can not be null.");
			return res;
		}

		if (message == null) {
			Global.get().setLastError("message to send is null.");
			return res;
		}

		try {
			if (!isSendChInited) {
				if (!initSendChannel()) {
					return CONSTS.REVOKE_NOK;
				}
			}

			sendBuilder.messageId(message.getMessageID());
			sendBuilder.timestamp(message.getTimeStamp());
			sendBuilder.deliveryMode(isDurable ? CONSTS.DELIVERY_MODE_DURABLE : CONSTS.DELIVERY_MODE_NO_DURABLE);
			
			int priority = message.getPriority();
			if (priority > 0) {
				sendBuilder.priority(priority > CONSTS.MQ_MAX_QUEUE_PRIORITY ? CONSTS.MQ_MAX_QUEUE_PRIORITY : priority);
			}

			sendProperties = sendBuilder.build();

			sendChannel.basicPublish("", queueName, CONSTS.AMQ_MANDATORY, CONSTS.AMQ_IMMEDIATE, sendProperties, message.getBody());
			
			if (isPubConfig) {
				try {
					boolean confirmOk = sendChannel.waitForConfirms();
					if (confirmOk) {
						int replyCode = retLock.get(CONSTS.REPLY_TIMEOUT);
						if (replyCode != AMQP.NO_ROUTE) {
							res = CONSTS.REVOKE_OK;
						} else {
							Global.get().setLastError("publishTopic error:" + sendRetLsnr.getReplyText());
						}
					}
				} catch (InterruptedException e) {
					logger.error(e.getMessage(), e);
					Global.get().setLastError(e.getMessage());
				} catch (TimeoutException e) {
					logger.error(e.getMessage(), e);
					Global.get().setLastError(e.getMessage());
				} finally {
					retLock.setFilled(false);
				}
			} else {
				res = CONSTS.REVOKE_OK;
			}

		} catch (Exception e) {
			// IOException AlreadyClosedException
			logger.error("sendQueue caught error, {}", e.getMessage());
			Global.get().setLastError(e.getMessage());

			res = CONSTS.REVOKE_NOK_NET_EXCEPTION;
		}

		return res;
	}

	/**
	 * 
	 * @param queueName
	 * @param message
	 * @return 0:SUCCESS -1:FAIL
	 */
	public int publishTopic(String topicName, MQMessage message, boolean isDurable) {
		int res = CONSTS.REVOKE_NOK;

		if (topicName == null) {
			Global.get().setLastError("topic can not be null.");
			return CONSTS.REVOKE_NOK;
		}

		if (message == null) {
			Global.get().setLastError("message to send is null.");
			return CONSTS.REVOKE_NOK;
		}

		try {
			if (!isSendChInited) {
				if (!initSendChannel()) {
					return CONSTS.REVOKE_NOK;
				}
			}

			sendBuilder.messageId(message.getMessageID());
			sendBuilder.timestamp(message.getTimeStamp());
			sendBuilder.deliveryMode(isDurable ? CONSTS.DELIVERY_MODE_DURABLE : CONSTS.DELIVERY_MODE_NO_DURABLE);

			sendProperties = sendBuilder.build();

			// mandatory和immediate标志说明
			// mandatory标志位设置为true时，如果exchange根据自身类型和消息routeKey无法找到一个符合条件的queue，
			// 那么会调用basic.return方法将消息返还给生产者;
			// 当mandatory设为false时，出现上述情形broker会直接将消息扔掉
			// 当immediate标志位设置为true时，如果exchange在将消息route到queue(s)时发现对应的queue上没有消费者，那么这条消息不会放入队列中
			// 当与消息routeKey关联的所有queue(一个或多个)都没有消费者时，该消息会通过basic.return方法返还给生产者
			sendChannel.basicPublish(CONSTS.AMQ_DIRECT, topicName, CONSTS.AMQ_MANDATORY, CONSTS.AMQ_IMMEDIATE, sendProperties,
					message.getBody());

			if (isPubConfig) {
				try {
					boolean confirmOk = sendChannel.waitForConfirms();
					if (confirmOk) {
						int replyCode = retLock.get(CONSTS.REPLY_TIMEOUT);
						if (replyCode != AMQP.NO_ROUTE) {
							res = CONSTS.REVOKE_OK;
						} else {
							Global.get().setLastError("publishTopic error:" + sendRetLsnr.getReplyText());
						}
					}
				} catch (InterruptedException e) {
					logger.error(e.getMessage(), e);
					Global.get().setLastError(e.getMessage());
				} catch (TimeoutException e) {
					logger.error(e.getMessage(), e);
					Global.get().setLastError(e.getMessage());
				} finally {
					retLock.setFilled(false);
				}
			} else {
				res = CONSTS.REVOKE_OK;
			}

		} catch (Exception e) {
			logger.error("publishTopic caught error, {}", e.getMessage());
			Global.get().setLastError(e.getMessage());

			res = CONSTS.REVOKE_NOK_NET_EXCEPTION;
		}

		return res;
	}

	/**
	 * 
	 * @param name
	 *            name of queue or topic
	 * @param message
	 * @param timeout
	 * @return 0:no message; 1:have rev a message; REVOKE_NOK: general
	 *         exception; REVOKE_NOK_SHUNDOWN: broker shutdown exception
	 */
	public int consumeMessage(String name, MQMessage message, int timeout) {
		int res = CONSTS.REVOKE_NOK;

		if (message == null) {
			Global.get().setLastError("message passed in is null.");
			return CONSTS.REVOKE_NOK;
		}

		QueueDtlBean qDtl = queueDtlMap.get(name);
		if (qDtl == null) {
			Global.get().setLastError("maybe queue not listened, listen first.");
			return CONSTS.REVOKE_NOK;
		}

		QueueingConsumer consumer = qDtl.getConsumer();
		if (consumer == null) {
			Global.get().setLastError("internal error, consuer null.");
			return CONSTS.REVOKE_NOK;
		}

		try {
			QueueingConsumer.Delivery delivery = consumer.nextDelivery(timeout);

			if (delivery == null) {
				res = 0;
			} else {
				int type = qDtl.getType();
				String srcQueueName = qDtl.getSrcQueueName();
				String realQueueName = qDtl.getRealQueueName();
				String consumerTag = qDtl.getConsumerTag();

				AMQP.BasicProperties properties = delivery.getProperties();
				message.setMessageID(properties.getMessageId());
				message.setBody(delivery.getBody());

				Long ts = properties.getTimestamp();
				if (ts != null)
					message.setTimeStamp(ts);

				Envelope envelope = delivery.getEnvelope();

				message.setDeliveryTagID(envelope.getDeliveryTag());
				message.setSourceName(srcQueueName);
				message.setSourceType(type);
				message.setRealQueueName(realQueueName);
				message.setConsumerTag(consumerTag);
				message.setNode(this);
				if (type == CONSTS.TYPE_TOPIC) {
					message.setConsumerId(qDtl.getConsumerId());
				}

				res = CONSTS.REVOKE_GETDATA;
			}
		} catch (Exception e) {
			if (e instanceof ShutdownSignalException
					|| e instanceof ConsumerCancelledException
					|| e instanceof AlreadyClosedException) {
				res = CONSTS.REVOKE_NOK_SHUNDOWN;
			}/* else if (e instanceof InterruptedException) {
				logger.error("consume is interrupt by other thread, stay interrupt until wakeup.");
				Thread.currentThread().interrupt();
			}*/

			logger.error("consumeMessage caught error, {}, {}", e.getMessage(), e.getClass());
			Global.get().setLastError(e.getMessage());
		}

		return res;
	}

	/*private int reListen(String name) {
		int res = CONSTS.REVOKE_NOK;

		QueueDtlBean qDtl = queueDtlMap.get(name);
		if (qDtl == null) {
			Global.get().setLastError("maybe queue not listened, listen first.");
			return CONSTS.REVOKE_NOK;
		}

		try {
			Channel dataChannel = qDtl.getDataChannel();
			if (!dataChannel.isOpen()) {
				int channelIdx = qDtl.getChannelIdx();
				dataChannel = conn.createChannel(channelIdx);
			}

			QueueingConsumer consumer = new QueueingConsumer(dataChannel);

			String currConsumerTag = qDtl.getConsumerTag();
			String serverConsumerTag = dataChannel.basicConsume(qDtl.getRealQueueName(), CONSTS.AUTO_ACK, currConsumerTag, consumer);
			if (serverConsumerTag.equals(currConsumerTag)) {
				qDtl.setDataChannel(dataChannel);
				qDtl.setConsumer(consumer);

				String info = String.format("reListen:%s success.", name);
				logger.info(info);
			} else {
				String err = String.format("reListen:%s failed.", name);
				logger.error(err);
				Global.get().setLastError(err);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		}

		return res;
	}*/

	/**
	 * 
	 * @param message
	 * @return 0:SUCCESS -1:FAIL
	 */
	public int ackMessage(MQMessage message) {
		int res = CONSTS.REVOKE_NOK;

		if (message == null) {
			Global.get().setLastError("message to ack is null.");
			return CONSTS.REVOKE_NOK;
		}

		QueueDtlBean qDtl = null;
		if (message.getSourceType() == CONSTS.TYPE_QUEUE) {
			String srcName = message.getSourceName();
			if (StringUtils.isNullOrEmtpy(srcName)) {
				Global.get().setLastError("message srcName is null or emputy.");
				return CONSTS.REVOKE_NOK;
			}
			qDtl = queueDtlMap.get(srcName);
		} else {
			String consumerId = message.getConsumerId();
			if (consumerId != null && !consumerId.equals("")) {
				qDtl = queueDtlMap.get(consumerId);
			} else {
				qDtl = queueDtlMap.get(message.getSourceName());
			}
		}
		if (qDtl == null) {
			Global.get().setLastError("maybe queue not listened, listen first.");
			return CONSTS.REVOKE_NOK;
		}

		Channel dataChannel = qDtl.getDataChannel();

		try {
			if (dataChannel != null) {
				dataChannel.basicAck(message.getDeliveryTagID(), CONSTS.ACK_MULTIPLE);
				res = CONSTS.REVOKE_OK;
			} else {
				Global.get().setLastError("dataChannel is null.");
			}

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		}

		return res;
	}

	/**
	 * 
	 * @param message
	 * @param requeue
	 *            是否重入队列
	 * @return 0:SUCCESS -1:FAIL
	 */
	public int rejectMessage(MQMessage message, boolean requeue) {
		int res = CONSTS.REVOKE_NOK;

		if (message == null) {
			Global.get().setLastError("message to ack is null.");
			return CONSTS.REVOKE_NOK;
		}

		String srcName = message.getSourceName();
		if (StringUtils.isNullOrEmtpy(srcName)) {
			Global.get().setLastError("message srcName is null or emputy.");
			return CONSTS.REVOKE_NOK;
		}

		QueueDtlBean qDtl = queueDtlMap.get(srcName);
		if (qDtl == null) {
			Global.get().setLastError("maybe queue not listened, listen first.");
			return CONSTS.REVOKE_NOK;
		}

		Channel dataChannel = qDtl.getDataChannel();

		try {
			if (dataChannel != null) {
				dataChannel.basicReject(message.getDeliveryTagID(), requeue);
			} else {
				Global.get().setLastError("dataChannel is null.");
			}

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Global.get().setLastError(e.getMessage());
		}

		return res;
	}

	public String getLocalAddr() {
		return localAddr;
	}

	public String getMQAddr() {
		Broker broker = vbroker.getMasterBroker();
		return String.format("%s:%d", broker.getIP(), broker.getPort());
	}
	
	public void cloneQueueDtlMap(Map<String, QueueDtlBean> cloneQueueDtlMap) {		
		Set<Entry<String, QueueDtlBean>> entrySet = queueDtlMap.entrySet();
		for (Entry<String, QueueDtlBean> entry : entrySet) {
			String key = entry.getKey();
			QueueDtlBean olderQueueTypeBean = entry.getValue();
			if (olderQueueTypeBean == null)
				continue;
			
			QueueDtlBean newQueueTypeBean = new QueueDtlBean(olderQueueTypeBean.getSrcQueueName(), olderQueueTypeBean.getType(),
					olderQueueTypeBean.getTopicType(), olderQueueTypeBean.getRealQueueName(), olderQueueTypeBean.getConsumerTag(),
					null, null, olderQueueTypeBean.getChannelIdx(), olderQueueTypeBean.getConsumerId());

			if (cloneQueueDtlMap != null)
				cloneQueueDtlMap.put(key, newQueueTypeBean);
		}
	}
	
	public void cloneBackupQueueDtlMap(Map<String, QueueDtlBean> cloneQueueDtlMap) {
		Set<Entry<String, QueueDtlBean>> entrySet = queueDtlBackupMap.entrySet();
		for (Entry<String, QueueDtlBean> entry : entrySet) {
			String key = entry.getKey();
			QueueDtlBean olderQueueTypeBean = entry.getValue();
			if (olderQueueTypeBean == null)
				continue;
			
			QueueDtlBean newQueueTypeBean = new QueueDtlBean(olderQueueTypeBean.getSrcQueueName(), olderQueueTypeBean.getType(),
					olderQueueTypeBean.getTopicType(), olderQueueTypeBean.getRealQueueName(), olderQueueTypeBean.getConsumerTag(),
					null, null, olderQueueTypeBean.getChannelIdx(), olderQueueTypeBean.getConsumerId());
			
			if (cloneQueueDtlMap != null)
				cloneQueueDtlMap.put(key, newQueueTypeBean);
		}
	}
	
	public void PutBackUpConsumerMap(Map<String, QueueDtlBean> cloneQueueDtlMap) {
		queueDtlBackupMap = cloneQueueDtlMap;
	}

}
