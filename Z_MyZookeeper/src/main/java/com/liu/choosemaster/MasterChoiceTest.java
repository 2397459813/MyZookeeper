package com.liu.choosemaster;

import org.apache.zookeeper.KeeperException;
import java.io.IOException;
import java.util.concurrent.*;

public class MasterChoiceTest {

    private final static String ZK_CONNECT_STRING="68.168.138.63:2181,68.168.138.63:2182,68.168.138.63:2183";
    private final static String ZK_ROOT_PATH="/zkmaster";
    private final static int SESSION_TIMEOUT=10000;
    private static final int THREAD_NUM=5;
    private static int threadNo=0;
    private static ExecutorService executorService=null;
    private static CountDownLatch threadCompleteLatch=new CountDownLatch(THREAD_NUM);

    public static void main(String[] args){

        executorService= Executors.newFixedThreadPool(THREAD_NUM, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {

                String name=String.format("The %s thread",++threadNo);
                Thread ret=new Thread(Thread.currentThread().getThreadGroup(),r,name,0);
                ret.setDaemon(false);
                return ret;
            }
        });
        if(executorService!=null){

            startProcess();
        }
    }

    private static void startProcess() {
    	//�����̳߳�Ҫִ�еľ�������
        Runnable task=new Runnable() {
            @Override
            public void run() {

                String threadName=Thread.currentThread().getName();
                ChooseMaster chooseMaster=new ChooseMaster(threadCompleteLatch);
                try {
                	//�ͻ������ӳɹ�֮�����ż���ִ��successCountDownLatch.await();
                    chooseMaster.createConnection(ZK_CONNECT_STRING,SESSION_TIMEOUT);
                    System.out.println(Thread.currentThread().getName()+" connected to server********");
                    //�����־ýڵ㣬������
                    synchronized (MasterChoiceTest.class){

                        chooseMaster.createPersistPath(ZK_ROOT_PATH,"thread "+threadName,true);
                    }
                    /*chooseMaster�������ݣ�
                     * 1����ѡ�����ڵ㴴����ʱ�ڵ�
                     * 2���ж��Ƿ������ڵ�,�����ڵ���ִ�����ڵ����񣬲������ڵ�������Լ���ǰһ���ڵ��ɾ���¼�
                     * 3��ִ�����ڵ�����ִ����Ͽͻ��˶Ͽ�������������ӣ����threadCompleteLatch.countDown()
                     * 
                     */
                    
                    chooseMaster.chooseMaster();
                    
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        for(int i=0;i<THREAD_NUM;i++){

            executorService.execute(task);
        }
        //�ȴ��̳߳��е�����ִ����ϣ��������ر��̳߳�
        executorService.shutdown();
        
        try {
            threadCompleteLatch.await();
            System.out.println("All thread finished");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}