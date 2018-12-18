package com.liu.connection;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class TestConnection {

	 // 会话超时时间，设置为与系统默认时间一致
    private static final int SESSION_TIMEOUT = 20 * 1000;
    private ZooKeeper zk;
    
    // 创建 Watcher 实例
    private Watcher wh = new Watcher() {
        /**
         * Watched事件
         */
    	@Override
        public void process(WatchedEvent event) {
            System.out.println("WatchedEvent >>>>>>>>>>>>>>> " + event.toString());
          //取得连接状态
    		KeeperState state = event.getState();
    	
    		//取得事件类型
    		EventType eventType = event.getType();
    		//哪一个节点路径发生变更
    		String nodePath = event.getPath();
    	
    		System.out.println("****"+state.name()+"****"+eventType+"*******"+nodePath);

        }


    };
    
    // 初始化 ZooKeeper 实例
    private void createZKInstance() throws IOException {
        // 连接到ZK服务，多个可以用逗号分割写
        zk = new ZooKeeper("68.168.138.63:2181,68.168.138.63:2182,68.168.138.63:2183", TestConnection.SESSION_TIMEOUT, this.wh);

    }
    
    
    private void ZKOperations() throws IOException, InterruptedException, KeeperException {
    	/*System.out.println("zookeeper操作。。。。。。。。。。。");
    	Stat stat = zk.exists("/zoo2", true);
    	if(stat==null) {
    		zk.create("/zoo2", "myData2".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    	}   	
    	System.out.println(new String(zk.getData("/zoo2", this.wh, null)));// 添加Watch    	
        System.out.println("\n3. 修改节点数据 ");
        zk.setData("/zoo2", "shanhy20160310".getBytes(), -1);
        // 这里再次进行修改，则不会触发Watch事件，这就是我们验证ZK的一个特性“一次性触发”，也就是说设置一次监视，只会对下次操作起一次作用。
        System.out.println("\n3-1. 再次修改节点数据 ");
        zk.setData("/zoo2", "shanhy20160310-ABCD".getBytes(), -1);
        System.out.println("\n4. 查看是否修改成功： ");
        System.out.println(new String(zk.getData("/zoo2", false, null)));
        System.out.println("\n5. 删除节点 ");
        zk.delete("/zoo2", -1);
        System.out.println("\n6. 查看节点是否被删除： ");
        System.out.println(" 节点状态： [" + zk.exists("/zoo2", true) + "]");*/
    	System.out.println(new String(zk.getData("/tasks", this.wh, null)));// 添加Watch 
        
    }
    
    public  List<String> getChildren(String path) throws KeeperException, InterruptedException{
		     List<String> children = zk.getChildren(path, false);
		     return children;
    }

    private void ZKClose() throws InterruptedException {
        zk.close();
    }
    
    
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    	TestConnection dm = new TestConnection();
        dm.createZKInstance();
        dm.ZKOperations();
        List<String> children = dm.getChildren("/tasks");
        System.out.println(children);

    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
}
