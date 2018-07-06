package model.mllib.mesPubSub;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


public class MessagePublicSubscribeZookeeper {
	private static ZooKeeper zk = null;
	public MessagePublicSubscribeZookeeper(){
		System.out.println("初始化除了zookeeper");
		}
	
	//初始化
	public static void init() throws Exception{
		System.out.println("开始初始化");
		//连接服务器
		final CountDownLatch cdl = new CountDownLatch(1);
		zk = new ZooKeeper("192.168.6.83:2181",1000*5,new Watcher(){

			@Override
			public void process(WatchedEvent event) {
				//org.apache.zookeeper.Watcher.Event.KeeperState--->Enumeration of states the Zookeeper may be at the event
				//SyncConnected代表客户端 和服务器已经连接成功了
				if(event.getState()==KeeperState.SyncConnected){
					cdl.countDown();
					System.out.println("初始化成功");
				}
				
				if(event.getType()==EventType.NodeChildrenChanged){
					System.out.println(event.getPath()+"子节点发生变化。。。。。。");
				}
				
				if(event.getType()==EventType.NodeDataChanged){
					System.out.println(event.getPath()+"子节点数据发生了变化。。。。。。");
				}
				
			}
			
		});
		
		cdl.await();
	}
	
	public static Stat checkNodeExist(String path) throws Exception {
		//检查节点是否存在
		Stat stat = zk.exists(path, false);
		System.out.println(stat);
		return stat;
	}
	public static Stat changeData(String path,String data) throws Exception{
		//修改数据
		Stat stat = zk.setData(path, data.getBytes(), -1);
		return stat;
	}
	public static String getData(String path) throws Exception{
		//获取节点数据,这里同步异步的方式，因为我们发送了请求后，为了保证数据正常返回，可以确认一下
		byte[] data = zk.getData(path, false, null);
		System.out.println("获取到的数据是"+data);
		return new String(data,"utf-8");
	}
	public static List<String> getChildrenList(String path) throws Exception {
		//获取子节点列表
		List<String> cs = zk.getChildren(path, new Watcher(){

			@Override
			public void process(WatchedEvent event) {
				System.out.println("/park子节点发生了变化");
				
			}
			
		});
		
		for(String c : cs){
			System.out.println(c);
		}
		return cs;
	}
	public static void deleteNode(String path) throws Exception {
		//删除节点,-1表示最新版本
		zk.delete(path, -1);
	}
	public static String createNode(String path,String data) throws Exception{
		System.out.println("开始创建节点"+path+"----"+data);
		//创建节点park
		String actualPath = zk.create(path, data.getBytes("utf-8"), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("创建好的节点是"+actualPath);
		return actualPath;
	}
}
