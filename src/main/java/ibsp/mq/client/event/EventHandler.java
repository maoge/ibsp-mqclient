package ibsp.mq.client.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibsp.common.utils.CONSTS;
import ibsp.mq.client.utils.Global;
import com.ffcs.nio.core.buffer.IoBuffer;
import com.ffcs.nio.core.core.impl.HandlerAdapter;
import com.ffcs.nio.core.core.Session;

public class EventHandler extends HandlerAdapter {
	
	private static Logger logger = LoggerFactory.getLogger(EventHandler.class);

	public EventHandler() {
		
	}
	
	@Override
	public void onMessageSent(Session session, Object msg)
    {
		
    }

	@Override
	public void onMessageReceived(Session session, Object msg) {
		if (msg == null)
			return;
		
		try {
			IoBuffer ioBuff = (IoBuffer) msg;
			
			int buffLen = ioBuff.limit();
			boolean headMatch = true;
			
			byte[] buffArr = ioBuff.array();
			if (buffLen > CONSTS.FIX_HEAD_LEN) {
				for (int i = 0; i < CONSTS.FIX_PREHEAD_LEN; i++) {
					if (buffArr[i] != CONSTS.PRE_HEAD[i]) {
						headMatch = false;
						break;
					}
				}
			} else {
				return;
			}
			
			if (!headMatch) {
				return;
			}
			
			int bodyLen = 0;
			bodyLen |= buffArr[CONSTS.FIX_PREHEAD_LEN];
			bodyLen |= buffArr[CONSTS.FIX_PREHEAD_LEN+1] << 8;
			bodyLen |= buffArr[CONSTS.FIX_PREHEAD_LEN+2] << 16;
			bodyLen |= buffArr[CONSTS.FIX_PREHEAD_LEN+3] << 24;
			
			if (buffLen < CONSTS.FIX_HEAD_LEN + bodyLen)
				return;
			
			EventMsg event = EventMsg.fronJson(buffArr, CONSTS.FIX_HEAD_LEN, bodyLen);
			if (event == null)
				return;
			
			logger.info("rev EventMsg:{}", event.toString());
			Global.get().pushEventMsg(event);
		} catch(Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

}
