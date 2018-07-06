package model.mllib.mesPubSub;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class demo {
	public static void main(String[] args) throws Exception {
		final CountDownLatch cdl = new CountDownLatch(1);
		ZooKeeper zk = new ZooKeeper("192.168.8.231:2182",1000*5,new Watcher(){

			@Override
			public void process(WatchedEvent event) {
				//org.apache.zookeeper.Watcher.Event.KeeperState--->Enumeration of states the Zookeeper may be at the event
				//SyncConnected代表客户端 和服务器已经连接成功了
				if(event.getState()==KeeperState.SyncConnected){
					cdl.countDown();
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
}
