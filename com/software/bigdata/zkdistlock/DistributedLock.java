package com.software.bigdata.zkdistlock;

import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * @Description: 分布式共享锁
 * 
 * @author Crawl
 * @date 2018年1月25日 下午5:02:42
 */
public class DistributedLock {
	
	private ZooKeeper zkClient = null;
	
	//连接字符串
	private static final String connectString = "zookeeper01:2181,zookeeper02:2181,zookeeper03:2181";
	
	//超时时间
	private static final int sessionTimeout = 2000;
	
	//父节点
	private static final String parentNode = "/locks";
	
	//记录自己创建子节点的路径
	private volatile String thisPath;
	
	public static void main(String[] args) throws Exception {
		//1.获取 ZooKeeper 的客户端连接
		DistributedLock distLock = new DistributedLock();
		distLock.getZKClient();
		
		//2.注册一把锁
		distLock.regiestLock();
		
		//3.监听父节点，判断是否只有自己在线
		distLock.watchParent();
	}
	
	//业务逻辑方法，注意：需要在最后释放锁
	public void dosomething() throws Exception {
		System.out.println("或得到锁:" + thisPath);
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			System.out.println("释放锁:" + thisPath);
			zkClient.delete(thisPath, -1);
		}
	}
	
	//监听父节点，判断是否只有自己在线
	public void watchParent() throws Exception {
		List<String> childrens = zkClient.getChildren(parentNode, true);
		if (childrens != null && childrens.size() == 1) {
			//只有自己在线，处理业务逻辑(处理完业务逻辑，必须删释放锁)
			dosomething();
		} else {
			//不是只有自己在线，说明别人已经获取到锁，等待
			Thread.sleep(Long.MAX_VALUE);
		}
	}
	
	//注册一把锁
	public void regiestLock() throws Exception {
		thisPath = zkClient.create(parentNode + "/lock", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
	}
	
	//获取 zk 客户端
	public void getZKClient() throws Exception {
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				//判断事件类型，只处理子节点变化事件
				if(event.getType() == EventType.NodeChildrenChanged && event.getPath().equals(parentNode)) {
					try {
						List<String> childrens = zkClient.getChildren(parentNode, true);
						//判断自己是否是最小的
						String thisNode = thisPath.substring((parentNode + "/").length());
						Collections.sort(childrens);
						if(childrens.indexOf(thisNode) == 0){
							//处理业务逻辑
							dosomething();
							//重新注册一把新的锁
							thisPath = zkClient.create(parentNode + "/lock", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});
	}

}
