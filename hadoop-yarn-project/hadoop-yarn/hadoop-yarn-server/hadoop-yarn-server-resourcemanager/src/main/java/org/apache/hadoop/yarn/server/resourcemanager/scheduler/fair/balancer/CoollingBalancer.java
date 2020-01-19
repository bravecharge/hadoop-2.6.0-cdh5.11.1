package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.balancer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerNode;
import org.apache.commons.logging.Log;

/**
 * <p>
 * 描述：简单的平衡器, 通过冷却时间来控制, 节点每次分配同一个程序后有600毫秒的冷却时间, 要等待冷却时间过后才能分配新的容器
 * </p>
 *
 * @author <a href="mailto:"yuxiaodong"@xiaoyouzi.com">"yuxiaodong"</a>
 * @date 2019年8月26日 上午10:14:54
 */

public class CoollingBalancer {

	static long coollingTime = 600;

	static ConcurrentHashMap<String, Long> cache = new ConcurrentHashMap<String, Long>();

	static {
		ExpireHandler eh = new ExpireHandler();
		Timer timer = new Timer();
		timer.schedule(eh, 300, 300);

	}

	/**
	 * 节点是否在冷却中
	 * 
	 * @param node
	 * @param app
	 * @return
	 */
	public static boolean isInCoolling(FSAppAttempt app, FSSchedulerNode node, Log LOG) {
		String key = app.getName() + "_" + node.getNodeName();
		if (cache.containsKey(key)) {
			return true;
		}
		long time = System.currentTimeMillis();
		cache.put(key, time);
		return false;
	}

	static class ExpireHandler extends TimerTask {
		@Override
		public void run() {
			try {
				Set<Entry<String, Long>> set = cache.entrySet();
				long now = System.currentTimeMillis();
				List<String> deleteKeys = new ArrayList<String>();
				for (Entry<String, Long> en : set) {
					if (now - en.getValue() > coollingTime) {
						deleteKeys.add(en.getKey());
					}
				}

				for (String key : deleteKeys) {
					cache.remove(key);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
