package com.liu.choosemaster;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/*
 * 
 * 所有的节点向zk的某个路径下注册，创建临时节点（临时节点，zookeeper会主动监控，一旦连接失效，zk会删除该临时节点），
 * 每个注册者创建时会有一个编号，每次选举编号最小的为主节点，其他节点就为从节点，从节点会监控主节点是否失效（怎么监控？ 
 * zk有事件，监听事件的状态变化，然后重新选举），为避免“惊群”现象，每个节点只监控比它小的一个临近节点。
 * 
 * 
 */
public class ChooseMaster implements Watcher {

	private ZooKeeper zk = null;
	private String selfPath = null;
	private String waitPath = null;
	private static final String ZK_ROOT_PATH = "/zkmaster"; // 选主从的根路径
	private static final String ZK_SUB_PATH = ZK_ROOT_PATH + "/register";
	private CountDownLatch successCountDownLatch = new CountDownLatch(1);
	private CountDownLatch threadCompleteLatch = null;

	public ChooseMaster(CountDownLatch countDownLatch) {

		this.threadCompleteLatch = countDownLatch;
	}

	@Override
	public void process(WatchedEvent watchedEvent) { // 监听事件

		Event.KeeperState keeperState = watchedEvent.getState();
		Event.EventType eventType = watchedEvent.getType();
		if (Event.KeeperState.SyncConnected == keeperState) { // 建立连接

			if (Event.EventType.None == eventType) {

				System.out.println(Thread.currentThread().getName() + " connected to server");
				successCountDownLatch.countDown();
				System.out.println(Thread.currentThread().getName()+"successCountDownLatch.getCount"+successCountDownLatch.getCount());
			} else if (Event.EventType.NodeDeleted == eventType && watchedEvent.getPath().equals(waitPath)) {
				
				// 监测到节点删除，且为当前线程的等待节点
				System.out.println(Thread.currentThread().getName()
						+ " some node was deleted,I'll check if I am the minimum node");
				try {

					if (checkMinPath()) { // 判断自己是不是最小的编号

						processMasterEvent(); // 处理主节点做的事情
					}
				} catch (Exception e) {

					e.printStackTrace();
				}

			}

		} else if (Event.KeeperState.Disconnected == keeperState) { // 连接断开

			System.out.println(Thread.currentThread().getName() + " release connection");
		} else if (Event.KeeperState.Expired == keeperState) { // 超时

			System.out.println(Thread.currentThread().getName() + " connection expire");
		}
	}

	public void chooseMaster() throws Exception {

		selfPath = zk.create(ZK_SUB_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL); // 创建临时节点
		System.out.println(Thread.currentThread().getName() + "create path " + selfPath);
		if (checkMinPath()) { // 判断是否为主节点

			processMasterEvent();
		}
	}

	public boolean createPersistPath(String path, String data, boolean needWatch)
			throws KeeperException, InterruptedException {

		if (zk.exists(path, needWatch) == null) {

			zk.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println(Thread.currentThread().getName() + " create persist path " + path);
		}
		return true;

	}

	public void createConnection(String connection, int timeout) throws IOException, InterruptedException {

		zk = new ZooKeeper(connection, timeout, this);
		successCountDownLatch.await();

	}

	private void processMasterEvent() throws KeeperException, InterruptedException {

		if (zk.exists(selfPath, false) == null) {

			System.out.println(Thread.currentThread().getName() + " selfnode is not exist " + selfPath);
			return;
		}
		System.out.println(Thread.currentThread().getName() + " I'm the master,now do work");
		Thread.sleep(2000);
		System.out.println(Thread.currentThread().getName() + " Finish do work,leave master");
		// zk.delete(selfPath,-1);
		releaseConnection();
		System.out.println(Thread.currentThread().getName() + " Finish do work,releaseConnection");
		threadCompleteLatch.countDown();
		System.out.println(Thread.currentThread().getName() + " threadCompleteLatch.getCount"+threadCompleteLatch.getCount());

	}

	private void releaseConnection() {

		if (zk != null) {

			try {
				zk.close();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private boolean checkMinPath() throws Exception {

        //获取根节点下的所有子节点，进行排序，取当前路径的index，如果排在第一个，则为主，否则检测前一个节点是否存在，不存在则重新选举最小的节点
		List<String> subNodes = zk.getChildren(ZK_ROOT_PATH, false);
		Collections.sort(subNodes);
		System.out.println(Thread.currentThread().getName()+"subNodes : "+subNodes.toString());
		System.out.println(Thread.currentThread().getName() + " tmp node index is "
				+ selfPath.substring(ZK_ROOT_PATH.length() + 1));
		int index = subNodes.indexOf(selfPath.substring(ZK_ROOT_PATH.length() + 1));
		System.out.println(Thread.currentThread().getName()+"index : "+index);
		switch (index) {

		case -1:
			System.out.println(Thread.currentThread().getName() + " create node is not exist");
			return false;
		case 0:
			System.out.println(Thread.currentThread().getName() + " I'm the master");
			return true;
		default:
			waitPath = ZK_ROOT_PATH + "/" + subNodes.get(index - 1);
			System.out.println(Thread.currentThread().getName() + " the node before me is " + waitPath);
			try {

				zk.getData(waitPath, true, new Stat());
				System.out.println(Thread.currentThread().getName() + "开始监控"+waitPath+"节点的删除事件");
				return false;
			} catch (Exception e) {

				if (zk.exists(waitPath, false) == null) {

					System.out.println(Thread.currentThread().getName() + " the node before me is not exist,now is me");
					return checkMinPath();
				} else {

					throw e;
				}
			}
		}

	}
}
