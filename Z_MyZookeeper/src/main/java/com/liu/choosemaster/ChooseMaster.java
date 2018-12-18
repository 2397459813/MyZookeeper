package com.liu.choosemaster;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/*
 * 
 * ���еĽڵ���zk��ĳ��·����ע�ᣬ������ʱ�ڵ㣨��ʱ�ڵ㣬zookeeper��������أ�һ������ʧЧ��zk��ɾ������ʱ�ڵ㣩��
 * ÿ��ע���ߴ���ʱ����һ����ţ�ÿ��ѡ�ٱ����С��Ϊ���ڵ㣬�����ڵ��Ϊ�ӽڵ㣬�ӽڵ�������ڵ��Ƿ�ʧЧ����ô��أ� 
 * zk���¼��������¼���״̬�仯��Ȼ������ѡ�٣���Ϊ���⡰��Ⱥ������ÿ���ڵ�ֻ��ر���С��һ���ٽ��ڵ㡣
 * 
 * 
 */
public class ChooseMaster implements Watcher {

	private ZooKeeper zk = null;
	private String selfPath = null;
	private String waitPath = null;
	private static final String ZK_ROOT_PATH = "/zkmaster"; // ѡ���ӵĸ�·��
	private static final String ZK_SUB_PATH = ZK_ROOT_PATH + "/register";
	private CountDownLatch successCountDownLatch = new CountDownLatch(1);
	private CountDownLatch threadCompleteLatch = null;

	public ChooseMaster(CountDownLatch countDownLatch) {

		this.threadCompleteLatch = countDownLatch;
	}

	@Override
	public void process(WatchedEvent watchedEvent) { // �����¼�

		Event.KeeperState keeperState = watchedEvent.getState();
		Event.EventType eventType = watchedEvent.getType();
		if (Event.KeeperState.SyncConnected == keeperState) { // ��������

			if (Event.EventType.None == eventType) {

				System.out.println(Thread.currentThread().getName() + " connected to server");
				successCountDownLatch.countDown();
				System.out.println(Thread.currentThread().getName()+"successCountDownLatch.getCount"+successCountDownLatch.getCount());
			} else if (Event.EventType.NodeDeleted == eventType && watchedEvent.getPath().equals(waitPath)) {
				
				// ��⵽�ڵ�ɾ������Ϊ��ǰ�̵߳ĵȴ��ڵ�
				System.out.println(Thread.currentThread().getName()
						+ " some node was deleted,I'll check if I am the minimum node");
				try {

					if (checkMinPath()) { // �ж��Լ��ǲ�����С�ı��

						processMasterEvent(); // �������ڵ���������
					}
				} catch (Exception e) {

					e.printStackTrace();
				}

			}

		} else if (Event.KeeperState.Disconnected == keeperState) { // ���ӶϿ�

			System.out.println(Thread.currentThread().getName() + " release connection");
		} else if (Event.KeeperState.Expired == keeperState) { // ��ʱ

			System.out.println(Thread.currentThread().getName() + " connection expire");
		}
	}

	public void chooseMaster() throws Exception {

		selfPath = zk.create(ZK_SUB_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL); // ������ʱ�ڵ�
		System.out.println(Thread.currentThread().getName() + "create path " + selfPath);
		if (checkMinPath()) { // �ж��Ƿ�Ϊ���ڵ�

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

        //��ȡ���ڵ��µ������ӽڵ㣬��������ȡ��ǰ·����index��������ڵ�һ������Ϊ����������ǰһ���ڵ��Ƿ���ڣ�������������ѡ����С�Ľڵ�
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
				System.out.println(Thread.currentThread().getName() + "��ʼ���"+waitPath+"�ڵ��ɾ���¼�");
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
