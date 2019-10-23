package org.apache.hadoop.yarn.server.resourcemanager.webapp;
/**
 * 判断ip是否为内网ip
 * 
 * @author yuxiaodong@xiaoyouzi.com
 *
 */
public class IPTools {

	public static boolean isInner(String ip) {
		ip = ip.replace(" ", "");
		long ip2long = int2long(str2Ip(ip));
//				  C类：192.168.0.0-192.168.255.255   局域网网段
//                B类：172.16.0.0-172.31.255.255
//                A类：10.0.0.0-10.255.255.255
		if ((ip2long >= 3232235520L && ip2long <= 3232301055L) || (ip2long >= 2886729728L && ip2long <= 2887778303L)
				|| (ip2long >= 167772160L && ip2long <= 184549375L) || ip2long == 0L) {
			return true;
		} else {
			return false;
		}
	}

	public static long int2long(int i) {
		long l = i & 0x7fffffffL;
		if (i < 0) {
			l |= 0x080000000L;
		}
		return l;
	}

	public static int str2Ip(String ip) {
		try {
			String[] ss = ip.split("\\.");
			int a, b, c, d;
			a = Integer.parseInt(ss[0]);
			b = Integer.parseInt(ss[1]);
			c = Integer.parseInt(ss[2]);
			d = Integer.parseInt(ss[3]);
			return (a << 24) | (b << 16) | (c << 8) | d;
		} catch (Exception e) {
			return 0;
		}
	}

	public static void main(String[] args) {
		String ip1 = "10.0.1.1";
		String ip2 = "220.33.22.11";
		String ip3 = "172.15.1.1";
		System.out.println(isInner(ip1));
		System.out.println(isInner(ip2));
		System.out.println(isInner(ip3));
	}
}
