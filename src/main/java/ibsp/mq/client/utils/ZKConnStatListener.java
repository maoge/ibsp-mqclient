package ibsp.mq.client.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKConnStatListener implements ConnectionStateListener {
	
	private static Logger logger = LoggerFactory.getLogger(ZKLocker.class);
	private String node = null;
	
	public ZKConnStatListener(String node) {
		this.node = node;
	}

	@Override
	public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
		
		if (connectionState == ConnectionState.RECONNECTED) {
			while (true) {
				try {
					if (curatorFramework.getZookeeperClient().blockUntilConnectedOrTimedOut()) {
						logger.info("zk event:reconnected, node:{}", node);
						
						if (curatorFramework.checkExists().forPath(node) == null) {
							curatorFramework.create()
									.creatingParentsIfNeeded()
									.withMode(CreateMode.EPHEMERAL)
									.forPath(node);
						}
						break;
					}
				} catch (InterruptedException e) {
					logger.error("connection block interrupted:{}", e.getMessage());
					break;
				} catch (Exception e) {
					logger.error("recreate temporary node:{} error, {}", node, e.getMessage());
				}
			}
		}
		
	}

}
