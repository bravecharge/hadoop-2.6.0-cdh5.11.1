package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.balancer;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerNode;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

/**
 * <p>
 * 描述：主要实现让container平衡分配在个个节点上,避免扎堆集中被分配在一个节点上
 * </p>
 *
 * @author <a href="mailto:"yuxiaodong"@xiaoyouzi.com">"yuxiaodong"</a>
 * @date 2019年8月23日 上午10:25:22
 */

public class AllocateBalancer {

	/**
	 * 改成map<appid,Map<nodeName, Nodeweight> 是否更好?
	 */
	private static ConcurrentHashMap<ApplicationId, List<NodeWeight>> appNodeGruops = new ConcurrentHashMap<ApplicationId, List<NodeWeight>>();

	private static AllocateBalancer INSTANCE = null;

	private static RMContext RMCONTEXT = null;

	private static final Log LOG = LogFactory.getLog(AllocateBalancer.class);

	private AllocateBalancer(RMContext rmContext) {
		RMCONTEXT = rmContext;

		// 缓存清理定时器
		Timer timer = new Timer();
		timer.schedule(new BlancerCleaner(), 120000, 120000);

		// web信息监控
		// init api server
		BalancerHttpWeb web = new BalancerHttpWeb();
		Server server = new Server(11250);
		Context context = new Context(server, "/balancer", Context.SESSIONS);
		context.addServlet(new ServletHolder(web), "/*");
		try {
			server.start();
//			server.join(); 同步模式
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static AllocateBalancer getInstance(RMContext rmContext) {
		if (INSTANCE == null) {
			synchronized (AllocateBalancer.class) {
				if (INSTANCE == null) {
					INSTANCE = new AllocateBalancer(rmContext);
				}
			}
		}
		return INSTANCE;
	}

	private NodeWeight getNodeWight(ApplicationId applicationId, FSSchedulerNode node) {
		List<NodeWeight> list = appNodeGruops.get(applicationId);
		if (list != null) {
			for (NodeWeight nw : list) {
				if (nw.getNode().getNodeName().equals(node.getNodeName())) {
					return nw;
				}
			}
		}
		return null;
	}

	/**
	 * 从程序的main args里寻找是否有参数enable_balancer 字符串标记, 有则使用平衡器
	 * 
	 * @param applicationId
	 * @param node
	 * @return
	 */
	public int getBalancerType(ApplicationId applicationId, FSSchedulerNode node) {
		RMAppAttempt appAttempt = RMCONTEXT.getRMApps().get(applicationId).getCurrentAppAttempt();
		List<String> commands = appAttempt.getSubmissionContext().getAMContainerSpec().getCommands();
		for (String cmd : commands) {
			cmd = cmd.replace("'", "");
			// 旧式兼容
			if (cmd.equals("enable_balancer_by_available")) {
				LOG.info(applicationId.toString() + " start to use balancer to assgin containers");
				return BalancerType.BY_AVAILABLE;
			} else if (cmd.equals("enable_balancer_by_total")) {
				return BalancerType.BY_TOTAL;
			}

			// 新
			if (cmd.contains("allocate.balancer.type")) {
				String[] pair = cmd.split("=");
				if (pair.length > 1) {
					String type = pair[1];
					if ("total".equals(type)) {
						return BalancerType.BY_TOTAL;
					} else if ("available".equals(type)) {
						return BalancerType.BY_AVAILABLE;
					}
				}
			}

		}
		return BalancerType.NONE;
	}

	/**
	 * 判断该节点是否有权重分配该程序容器
	 * 
	 * @param applicationId
	 * @param requestNode
	 * @return
	 */
	public synchronized boolean isInTurn(ApplicationId applicationId, FSSchedulerNode requestNode, int balancerType) {
		List<NodeWeight> list = appNodeGruops.get(applicationId);
		if (list == null) {
			list = new ArrayList<NodeWeight>();
			appNodeGruops.put(applicationId, list);
		}

		// 判断该node是否已经加入list
		NodeWeight currentNode = getNodeWight(applicationId, requestNode);
		if (currentNode == null) {
			currentNode = new NodeWeight(requestNode, applicationId, balancerType);
			list.add(currentNode);
		}

		// 此段信息更新操作必须在 sort之前
		currentNode.increaseRount();// 更新轮询次数, 轮询请求次数+1

		if (currentNode.getRound() < 5) {// TODO 待定 为了尽量收齐各个node 第五轮后才开始进入分配队列 是否让 assignPreemptedContainers 里完成这个操作合理点?
			if (LOG.isDebugEnabled()) {
				LOG.debug(currentNode.getNode() + " round:" + currentNode.getRound()
						+ " is < minimum limit, wait next round");
			}
			return false;
		}

		int topNodeNum = 1;// 默认只有最高权重的一个节点可以分配到
		if (currentNode.getRound() > 40 && currentNode.getRound() <= 60) { // 如果超过30轮都还没分配完, 可能是第一权重的node出了问题, 分配权从第一
			topNodeNum = 3;
			LOG.info(requestNode.getNodeName() + ":" + applicationId + " increase top weight node to " + topNodeNum);
		}
		if (currentNode.getRound() > 60) {// 如果50轮都还没分配完, 则放弃权重分配
			topNodeNum = list.size();
			LOG.info(requestNode.getNodeName() + ":" + applicationId + " quit from balancer");
		}
//		currentNode.update(); // 更新

		// 根据权重排序, 权重越大越靠前
		sortAndUpdate(list);

		boolean result = false;
		for (int i = 0; i < list.size(); i++) {
			NodeWeight nw = list.get(i);
			if (nw.getNode().getNodeName().equals(requestNode.getNodeName())) {
				result = true;
				break;
			}

			// 如果下个节点的权重跟当前一样 则继续遍历
			if (i < (list.size() - 1)) {
				double nextWeight = list.get(i + 1).getWeight();
				if (nextWeight == nw.getWeight()) {
					topNodeNum++;
					continue;
				}
			}

			// 超出topN就停止遍历
			if (i >= (topNodeNum - 1)) {
				break;
			}
		}
		return result;
	}

	public void update(ApplicationId applicationId, FSSchedulerNode node) {
		NodeWeight n = getNodeWight(applicationId, node);
		n.update();
	}

	private static void expire() {
		if (RMCONTEXT == null) {
			return;
		}
		ConcurrentMap<ApplicationId, RMApp> rmApps = RMCONTEXT.getRMApps();
		Set<ApplicationId> balanceApps = appNodeGruops.keySet();
		Set<ApplicationId> expireBalanceApps = new HashSet<ApplicationId>();
		for (ApplicationId appcationId : balanceApps) {
			RMApp rmApp = rmApps.get(appcationId);
			if (!FinalApplicationStatus.UNDEFINED.equals(rmApp.getFinalApplicationStatus())
					|| FinalApplicationStatus.FAILED.equals(rmApp.getFinalApplicationStatus())
					|| FinalApplicationStatus.KILLED.equals(rmApp.getFinalApplicationStatus())
					|| FinalApplicationStatus.SUCCEEDED.equals(rmApp.getFinalApplicationStatus()) || rmApp == null) {
				expireBalanceApps.add(appcationId);
			}
		}

		for (ApplicationId appcationId : expireBalanceApps) {
			LOG.info("expire " + appcationId.toString() + " info in balancer");
			appNodeGruops.remove(appcationId);
		}
	}

	/**
	 * 根据权重,对节点列表排序
	 * 
	 * @param list
	 * @return
	 */
	private synchronized List<NodeWeight> sortAndUpdate(List<NodeWeight> list) {
		Collections.sort(list, new Comparator<NodeWeight>() {
			@Override
			public int compare(NodeWeight o1, NodeWeight o2) {
				o1.update();
				o2.update();
				return (int) (o2.getWeight() - o1.getWeight());
			}
		});
		return list;
	}

	private List<NodeWeight> sortOnly(List<NodeWeight> list) {
		Collections.sort(list, new Comparator<NodeWeight>() {
			@Override
			public int compare(NodeWeight o1, NodeWeight o2) {
				return (int) (o2.getWeight() - o1.getWeight());
			}
		});
		return list;
	}

	static class BlancerCleaner extends TimerTask {
		@Override
		public void run() {
			expire();
		}
	}

	/**
	 * 提供http接口,访问数据
	 * 
	 */
	class BalancerHttpWeb extends HttpServlet {
		private static final long serialVersionUID = -598889262631166853L;
		private static final String CONTENT_TYPE = "text/plain";

		@Override
		protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
			resp.setStatus(HttpServletResponse.SC_OK);
			resp.setHeader("Cache-Control", "must-revalidate,no-cache,no-store");
			resp.setContentType(CONTENT_TYPE);
			final PrintWriter writer = resp.getWriter();
			try {
				if (req.getPathInfo().equals("/info")) {
					Set<Entry<ApplicationId, List<NodeWeight>>> set = appNodeGruops.entrySet();
					for (Entry<ApplicationId, List<NodeWeight>> en : set) {
						String line = en.getKey().toString() + ":";
						List<NodeWeight> list = en.getValue();
						sortAndUpdate(list);// TODO 此处监控是否显示当时的场景状态, 还是显示目前状态
						for (NodeWeight nw : en.getValue()) {
							line += "\n\t" + nw.toJson().toString();
						}
						writer.println(line + "\n");
					}
				} else if (req.getPathInfo().equals("/expire")) {
					LOG.info("do expire the balancer");
					expire();
					writer.println("do expire success");
				} else if (req.getPathInfo().equals("/empty")) {
					LOG.info("do empty the balancer");
					appNodeGruops.clear();
					writer.println("do empty success");
				}
			} finally {
				writer.close();
			}
		}
	}
}
