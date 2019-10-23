改动:
一. hadoop-yarn-server-resourcemanage
1.节点屏蔽功能

	 --node.black.list=n1,n2,n3     在main args上加上该参数, 则屏蔽n1,n2,n3节点,容器将不再分配在这些节点上

2.基于vcores平衡分配

	--allocate.balancer.type=total 或 --allocate.balancer.type=available	在main args上加上该参数,
	    "total"容器基于总资源平衡分配,   
	    "available" 容器基于当前可用的资源平衡分配  详细见:http://wiki.meiyou.com/pages/viewpage.action?pageId=39912106
	  

3.resource manager http添加basic auth认证

	在yarn-site.xml中设置  http.basic.auth.enabled=true 则开启认证
   	 http.basic.auth=a:b  则指定账号为a密码为b
    
