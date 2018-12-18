package com.liu.distributedlock;

public class Test {
    static int n = 500;

    public static void secskill() {
        System.out.println(--n);
    }

    public static void main(String[] args) {
        
        Runnable runnable = new Runnable() {
            public void run() {
                DistributedLock lock = null;
                try {
                    lock = new DistributedLock("68.168.138.63:2181,68.168.138.63:2182,68.168.138.63:2183", "test1");
                    //�ֲ�ʽ��,�������н��̶��ɼ�
                    lock.lock();
                    secskill();
                    System.out.println(Thread.currentThread().getName() + "��������");
                } finally {
                    if (lock != null) {
                        lock.unlock();
                    }
                }
            }
        };

        for (int i = 0; i < 10; i++) {
            Thread t = new Thread(runnable);
            t.start();
        }
    }
}
