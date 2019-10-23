package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * <p>
 * 描述：主要实现让container平衡分配在个个节点上,避免扎堆集中被分配在一个节点上
 * </p>
 *
 * @author <a href="mailto:"yuxiaodong"@xiaoyouzi.com">"yuxiaodong"</a>
 * @date 2019年8月23日 上午10:25:22
 */

public class BalanceManager {

	class AllocateInfo {
		/**
		 * application id
		 */
		String appId;
		/**
		 * nodemanger 节点名称
		 */
		String node;
		/**
		 * 该node第几轮来获取容器
		 */
		int round;
		/**
		 * 该程序已经有多少个程序被分配在了该节点上
		 */
		int allotNum;

		/**
		 * cpu核数, 代表了节点的计算能力, 更大的cpu核数应该获得更高的分配权重
		 */
		int cores;

		/**
		 * 内存空间, yarn内置会判断此处不需要以此做考虑
		 */
//		int memory;

	}

	/**
	 * 基础权重, 100000以上代表container分配权重,  10000以下代表轮训次数权重
	 * 机器每
	 */
	static long BASE_WEIGHT = 10001000;
	/**
	 * cpu核数权重单位, 以多少个vcores为一个权重单位系数
	 */
	static int CORES_UNIT_WEIGHT = 4;

	private long initWeight(FSSchedulerNode node) {
		int vcores = node.getTotalResource().getVirtualCores();
		int coresWeightMod = Math.round(vcores / CORES_UNIT_WEIGHT);
		if (coresWeightMod <= 0) {
			coresWeightMod = 1;
		}
		long weight = coresWeightMod * 10000 + BASE_WEIGHT;
		return weight;
	}

	public static void main(String[] args) {
		/**
		 * 初始值:Math.round(vcores/12) * 10000 + 10001000 
		 */
		long default_weight = 10001000;

		Map<String, TreeMap<Long, AllocateInfo>> balanceMap = new HashMap<String, TreeMap<Long, AllocateInfo>>();

//		map<String, TreeMap>
//			applicationid -> treemap<long(权重), AllocateInfo>

		TreeMap<Integer, String> map = new TreeMap<>();
		map.put(2, "2");
		map.put(4, "4");
		map.put(3, "3");
		map.put(1, "1");

		Set<Integer> set = map.descendingKeySet();
		for (Integer i : set) {
			System.out.println(map.get(i));
		}
	}
}
