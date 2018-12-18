package com.liu;

import java.util.concurrent.CountDownLatch;

public class TestCountDownLatch {
	public static void main(String[] args) {
		testCountDownLatch();
	}

	public static void testCountDownLatch() {

		int threadCount = 10;

		final CountDownLatch latch = new CountDownLatch(10);

		for (int i = 0; i < threadCount; i++) {

			new Thread(new Runnable() {

				@Override
				public void run() {

					System.out.println("�߳�" + Thread.currentThread().getId() + "��ʼ����");

					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					System.out.println("�߳�" + Thread.currentThread().getId() + "�ѵ����յ�");

					latch.countDown();
				}
			}).start();
		}

		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("10���߳��Ѿ�ִ����ϣ���ʼ��������");
	}
}
