package ibsp.mq.client.config;

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

import ibsp.common.events.EventController;
import ibsp.common.events.EventSubscriber;
import ibsp.common.utils.CONSTS;
import ibsp.common.utils.MetasvrUrlConfig;
import ibsp.mq.client.event.EventMsg;
import ibsp.mq.client.router.Router;
import ibsp.mq.client.utils.Global;

public class MetasvrConfigFactory implements EventSubscriber {
	
	private static final Logger logger = LoggerFactory.getLogger(MetasvrConfigFactory.class);
	
	private static final ReentrantLock monitor = new ReentrantLock();
	private static MetasvrConfigFactory instance = null;
	
	public static MetasvrConfigFactory getInstance() {
		return instance;
	}
	
	public static MetasvrConfigFactory getInstance(String metasvrUrl) {
		monitor.lock();
		try {
			if (instance == null) {
				instance = new MetasvrConfigFactory(metasvrUrl);
			}
		} finally {
			monitor.unlock();
		}
		return instance;
	}
	
	private MetasvrConfigFactory(String metasvrUrl) {
		EventController.getInstance().subscribe(CONSTS.TYPE_MQ_CLIENT, this);
	}

	@Override
	public void postEvent(JSONObject json) {
		EventMsg event = EventMsg.fromJson(json);
		Global.get().dispachEventMsg(event);
		
		logger.info("mq event:{}", json);
	}

	@Override
	public void doCompute() {
		Map<String, Router> routerMap = Global.get().getRouterMap();
		if (routerMap == null)
			return;
		
		Set<Entry<String, Router>> entrySet = routerMap.entrySet();
		for (Entry<String, Router> entry : entrySet) {
			Router router = entry.getValue();
			router.doCompute();
		}
	}

	@Override
	public void doReport() {
		Map<String, Router> routerMap = Global.get().getRouterMap();
		if (routerMap == null)
			return;
		
		Set<Entry<String, Router>> entrySet = routerMap.entrySet();
		for (Entry<String, Router> entry : entrySet) {
			Router router = entry.getValue();
			router.doReport();
		}
	}
	
	public synchronized void close() {
		// unsubscribe and stop event controller
		EventController.getInstance().unsubscribe(CONSTS.TYPE_MQ_CLIENT);
		EventController.getInstance().shutdown();

		MetasvrUrlConfig.get().close();
		
		if (instance!=null) {
			instance = null;
		}
	}

}
