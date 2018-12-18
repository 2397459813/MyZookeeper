package com.liu.listeren;

import java.io.IOException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class MoreListeren implements Watcher {
	private static ZooKeeper zk = null;
	private final static String ZK_CONNECT_STRING = "68.168.138.63:2181,68.168.138.63:2182,68.168.138.63:2183";
	private final static int SESSION_TIMEOUT = 10000;

	public static void main(String[] args) throws KeeperException {
		try {
			MoreListeren listern = new MoreListeren();
			listern.createConnection(ZK_CONNECT_STRING, SESSION_TIMEOUT);
			zk.getData("/zk_listeren", true, new Stat());
			System.out.println("�ͻ��˴���/zk_listeren���ݸ��¼����ɹ���������������������");
			while (true) {
				Thread.sleep(5000);
			}

		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void createConnection(String connection, int timeout) throws IOException, InterruptedException {

		zk = new ZooKeeper(connection, timeout, this);

	}

	@Override
	public void process(WatchedEvent watchedEvent) {
		// TODO Auto-generated method stub

		Event.KeeperState keeperState = watchedEvent.getState();
		if (Event.KeeperState.SyncConnected == keeperState) {

			System.out.println("������/zk_listeren�ڵ����ݸı䣬���������ü���������������������");//���ӷ���ɹ��ᴥ��һ��
			try {
				zk.getData("/zk_listeren", true, new Stat());//�������ü���
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (Event.KeeperState.Disconnected == keeperState) { // ���ӶϿ�

			System.out.println(Thread.currentThread().getName() + " release connection");
		} else if (Event.KeeperState.Expired == keeperState) { // ��ʱ

			System.out.println(Thread.currentThread().getName() + " connection expire");
		}

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
}
