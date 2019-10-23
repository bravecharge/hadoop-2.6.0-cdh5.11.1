package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.balancer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import com.google.gson.JsonObject;

/**
 * <p>
 * 描述：节点权重信息
 * </p>
 *
 * @author <a href="mailto:"yuxiaodong"@xiaoyouzi.com">"yuxiaodong"</a>
 * @date 2019年8月23日 上午10:25:22
 */

public class NodeWeight {
	private static final Log LOG = LogFactory.getLog(NodeWeight.class);
	// 常量区
	/**
	 * 基础权重
	 */
	private static double BASE_WEIGHT = 100000.0;

	/**
	 * cpu核数权重单位, 以多少个vcores为一个权重单位系数
	 */
//	private static double CORES_UNIT = 4.0;

	/**
	 * 每个cpu计算单位的权重系数值
	 */
	private static double CORES_FACTOR = 100.0;

	/**
	 * 每过1轮请求的衰减值
	 */
	private static double ROUND_FACTOR = 10;

	/**
	 * 每分配到1个container的衰减值
	 */
	private static double CONTAINER_FACTOR = 1000;

	// 信息字段区
	/**
	 * 任务id
	 */
	private ApplicationId applicationId;

	/**
	 * 该node第几轮来获取容器
	 */
	private int round;
	/**
	 * 该程序已经有多少个程序被分配在了该节点上
	 */
	private int containers;

	/**
	 * 权重参考cores的类型 1参考vvailableCores, 2参考node的totalCores
	 */
	private int coresType ;

	/**
	 * 已分配cores
	 */
	private int useCores;
	
	/**
	 *  下一次分配参考的活跃cores
	 */
	private int nextAvailableCores;

	private SchedulerNode node;

	/**
	 * 当前权重
	 */
	private double weight;

	public NodeWeight(SchedulerNode node, ApplicationId applicationId, int balancerType) {
		this.round = 0;
		this.containers = 0;
		this.applicationId = applicationId;
		this.node = node;
		this.coresType = balancerType;
	}

	public int getContainers() {
		return this.containers;
	}

	public int getRound() {
		return this.round;
	}

	public SchedulerNode getNode() {
		return this.node;
	}

	public void increaseRount() {
		this.round++;
	}

	/**
	 * 获取最新的参与计算权重的cores个数
	 * 
	 * @return
	 */
	public int getCountCores() {
		if (coresType == 1) {
			return node.getAvailableResource().getVirtualCores();
		} else if (coresType == 2) {
			return node.getTotalResource().getVirtualCores();
		}
		return node.getAvailableResource().getVirtualCores();
	}
	
	/**
	 * 计算权重
	 */
	public double getWeight() {
		double baseWeight = (getCountCores() * CORES_FACTOR) + BASE_WEIGHT;// 基础权重
		// 放弃round作为权重因素
//		baseWeight -= round * ROUND_FACTOR;//TODO 此处待定 轮询次数越多是应该权重越大合适还是越小合适呢?  
		if (coresType == BalancerType.BY_TOTAL) {
			baseWeight -= useCores * CORES_FACTOR;// total模式才需要减去该app已使用的cores的权重值
		}
		baseWeight -= (containers * CONTAINER_FACTOR);// TODO 此处待完善 为了避免将同个任务的多个容器落在一个节点上, 是否将已分配的containers 也作为权重因素?
		this.weight = baseWeight;
		return this.weight;
	}

	/**
	 * 更新节点分配信息
	 */
	public synchronized void update() {
		int _containers = 0;
		int _useCores = 0;
		for (RMContainer rmc : node.getRunningContainers()) {
			String id = getIdFromHaContainerId(rmc.getContainer().getId().toString());
			if (id.equals(this.applicationId.toString().substring(12))) {
				_containers += 1;
				_useCores += rmc.getContainer().getResource().getVirtualCores();
			}
			this.containers = _containers;
			this.useCores = _useCores;
			this.nextAvailableCores = getCountCores();
		}
	}
	
	/**
	 * 从containerid找到appid, 注意单点模式下 containerid为: container_e14_1567490818119_0001_01_000001, 而高可用模式下为: container_1567490818119_0001_01_000001
	 * @param containerId
	 * @return
	 */
	private String getIdFromHaContainerId(String containerId) {
		String[] items = containerId.split("_");
		String id = items[2] + "_" + items[3];
		return id;
	}
	
	/**
	 * 从containerid找到appid, 注意单点模式下 containerid为: container_e14_1567490818119_0001_01_000001, 而高可用模式下为: container_1567490818119_0001_01_000001
	 * @param containerId
	 * @return
	 */
	private String getIdFromContainerId(String containerId) {
		String[] items = containerId.split("_");
		String id = items[1] + "_" + items[2];
		return id;
	}


	public JsonObject toJson() {
		JsonObject json = new JsonObject();
		json.addProperty("node", node.getNodeName());
		json.addProperty("containers", containers);
		json.addProperty("round", round);
		json.addProperty("weight", getWeight());
		json.addProperty("useCores", useCores);
		json.addProperty("coresType", coresType);
		json.addProperty("nextAvailableCores", nextAvailableCores);
		return json;
	}
	
	public static void main(String[] args) {
		String cid = "container_e14_1567490818119_0001_01_000001";
		String[] items = cid.split("_");
		String id = items[2] + "_" + items[3];
		System.out.println(id);
	}

}