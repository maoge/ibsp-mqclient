package ibsp.mq.client.api;

public interface IMQClient {

	/**
	 * 方法说明：设置认证的账户和密码
	 * 
	 * @param userID
	 * @param userPWD
	 */
	public void setAuthInfo(String userID, String userPWD);
	
	/**
	 * 方法说明：与queueName对应所有vbroker建立连接(只连接vbroker中当前主节点。
	 * 
	 * @param queueName
	 * @return 0:success !0:error
	 */
	public int connect(String queueName);

	/**
	 * 方法说明：关闭客户端连接
	 * 
	 */
	public void close();
	
	public void setQos(int qos);

	/**
	 * 方法说明：返回最后的错误信息
	 * 
	 * @return
	 */
	public String GetLastErrorMessage();

	/**
	 * 方法说明：创建名称为queueName的队列，durable是否要求消息持久化， groupId指定创建在哪个组上，type 1:queue
	 * 2:topic
	 * 
	 * @param queueName
	 * @param durable
	 * @param groupId
	 * @param type
	 * @return 0:success !0:error
	 */
	public int queueDeclare(String queueName, boolean durable, String groupId, int type);
	
	/**
	 * 方法说明：创建名称为queueName的队列，durable是否要求消息持久化， ordered是否要求全局有序，groupId指定创建在哪个组上，type 1:queue
	 * 2:topic
	 * 
	 * @param queueName
	 * @param durable
	 * @param ordered
	 * @param groupId
	 * @param type
	 * @return 0:success !0:error
	 */
	public int queueDeclare(String queueName, boolean durable, boolean ordered, String groupId, int type);
	
	/**
	 * 方法说明：创建名称为queueName的队列，durable是否要求消息持久化， ordered是否要求全局有序，
	 *         priority是否优先级队列, groupId指定创建在哪个组上，
	 *         type 1:queue,2:topic
	 * 
	 * @param queueName
	 * @param durable
	 * @param ordered
	 * @param groupId
	 * @param type
	 * @return 0:success !0:error
	 */
	public int queueDeclare(String queueName, boolean durable, boolean ordered, boolean priority, String groupId, int type);

	/**
	 * 方法说明：删除名称为queueName队列
	 * 
	 * @param queueName
	 * @return 0:success !0:error
	 */
	public int queueDelete(String queueName);

	/**
	 * 方法说明：开始对队列queueName进行监听，之后可通过consumeMessage收取消息
	 * 
	 * @param queueName
	 * @return 0:success !0:error
	 */
	public int listenQueue(String queueName);

	/**
	 * 方法说明：对已经监听的队列queueName取消监听
	 * 
	 * @param queueName
	 * @return 0:success !0:error
	 */
	public int unlistenQueue(String queueName);

	/**
	 * 方法说明：匿名方式订阅topic的消息，之后可通过consumeMessage收取消息
	 * 如果连接中断，中断的时间窗口内消息会丢失，如果业务不考虑则使用这种方式，
	 * 使用这种方式的好处：连接中断匿名队列自动删除，不会产生消息堆积在匿名队列上
	 * 
	 * @param topic
	 * @return 0:success !0:error
	 */
	public int listenTopicAnonymous(String topic);

	public int unlistenTopicAnonymous(String topic);

	/**
	 * 方法说明：持久化方式订阅topic的消息
	 * 如果连接中断，中断的时间窗口内消息不会丢失，发送到topicName的消息会自动投递到consuerId对应的队列中
	 * 保证连接中断不丢消息，如果不用了需要做两件事先解绑再删除逻辑队列(logicQueueDelete)
	 * 消费ID不能复用，要不然就不能保证每条消息都能被消费
	 * 
	 * @param topicName
	 * @param consumerId
	 * @return 0:success !0:error
	 */
	public int listenTopicPermnent(String topic, String consumerId);

	public int unlistenTopicPermnent(String consumerId);

	/**
	 * 方法说明：扩展通配方式, 统一通道发送, 过滤接收, 真实持久化队列与consumerId绑定, 确保闪断不丢消息 mainKey如:123.*
	 * subKey如:123.1
	 * 
	 * @param mainKey
	 * @param subKey
	 * @param consumerId
	 * @return
	 */
	public int listenTopicWildcard(String mainKey, String subKey, String consumerId);

	public int unlistenTopicWildcard(String consumerId);

	/**
	 * 方法说明：生成一个全新的消费ID，listenTopicPermernent需要传入消费ID
	 * 注意如果消费ID不用了一定要logicQueueDelete， 要不然消息会一直堆积在中转队列中
	 * 
	 * @return null:error
	 */
	public String genConsumerId();

	/**
	 * 方法说明：删除持久化方式订阅topic的逻辑队列
	 * 
	 * @param consumerId
	 * @return 0:success !0:error
	 */
	public int logicQueueDelete(String consumerId);

	/**
	 * 方法说明：指定队列queueName发送消息
	 * 
	 * @param queueName
	 *            in 目标队列名
	 * @param message
	 *            in 待发消息对象
	 * @return 0:success !0:error
	 */
	public int sendQueue(String queueName, MQMessage message);
	
	/**
	 * 方法说明：往topic发布消息，消息会发送到给所有订阅者
	 * 
	 * @param topic
	 * @param message
	 * @return 0:success !0:error
	 */
	public int publishTopic(String topic, MQMessage message);

	/**
	 * 方法说明：扩展通配方式对应的发送方法, main_key如:123.* sub_key如:123.1
	 * 
	 * @param mainKey
	 * @param subKey
	 * @param message
	 * @return
	 */
	public int publishTopicWildcard(String mainKey, String subKey, MQMessage message);
	
	/**
	 * 方法说明：不指定queue/topic名接收消息
	 * 
	 * @param message
	 *            out 收到的对象
	 * @param timeout
	 *            in 超时时间(毫秒) -1代表无超时，0则不等待
	 * @return 0:未接收到消息 1:接收到1条消息 else:error
	 */
	public int consumeMessage(MQMessage message, int timeout);
	
	
	/**
	 * 方法说明：指定queue/topic名接收消息, 如果是permnent或wildcard方式name传consumerId
	 * 
	 * @param name
	 *            in name of queue/topic
	 * @param message
	 *            out 收到的对象
	 * @param timeout
	 *            in 超时时间(毫秒) -1代表无超时，0则不等待
	 * @return 0:未接收到消息 1:接收到1条消息 else:error
	 */
	public int consumeMessage(String name, MQMessage message, int timeout);

	/**
	 * 方法说明：通配方式消费消息
	 * 
	 * @param mainKey
	 *            in 带通配的topic key, 如abc.*
	 * @param subKey
	 *            in 实际真实topic key, 如abc.1
	 * @param message
	 *            out 收到的对象
	 * @param timeout
	 *            in 超时时间(毫秒) -1代表无超时，0则不等待
	 * @return 0:未接收到消息 1:接收到1条消息 else:error
	 */
	public int consumeMessageWildcard(String mainKey, String subKey, String consumerId, MQMessage message, int timeout);

	/**
	 * 方法说明：消费者签收消息，只有调用该函数后，消息才真正从队列中清除
	 * 
	 * @param message
	 * @return 0:success !0:error
	 */
	public int ackMessage(MQMessage message);

	/**
	 * 方法说明：消费者拒绝消息，reQueue为true时消息消息重新回到队列，为false时从队列删除
	 * 
	 * @param message
	 * @param reQueue
	 * @return 0:success !0:error
	 */
	public int rejectMessage(MQMessage message, boolean reQueue);
	
	/**
	 * 方法说明：清除队列数据
	 * 
	 * @param queueName
	 */
	public int purgeQueue(String queueName);
	
	/**
	 * 方法说明：清除Permnent和Wildcard两种方式的TOPIC对应的数据, Anonymous方式不需要purge,直接unlistenTopicAnonymous或close自动清除
	 * 
	 * @param consumerId
	 */
	public int purgeTopic(String consumerId);
	
	/**
	 * 方法说明：查询队列当前在途的总消息数
	 * 
	 * @param name
	 * @return
	 */
	public int getMessageReady(String name);

	@Deprecated
	public void startTx();

	@Deprecated
	public void commitTx();

}
