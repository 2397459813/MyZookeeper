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
    	//定义线程池要执行的具体任务
        Runnable task=new Runnable() {
            @Override
            public void run() {

                String threadName=Thread.currentThread().getName();
                ChooseMaster chooseMaster=new ChooseMaster(threadCompleteLatch);
                try {
                	//客户端连接成功之后程序才继续执行successCountDownLatch.await();
                    chooseMaster.createConnection(ZK_CONNECT_STRING,SESSION_TIMEOUT);
                    System.out.println(Thread.currentThread().getName()+" connected to server********");
                    //创建持久节点，加类锁
                    synchronized (MasterChoiceTest.class){

                        chooseMaster.createPersistPath(ZK_ROOT_PATH,"thread "+threadName,true);
                    }
                    /*chooseMaster方法内容：
                     * 1：在选主根节点创建临时节点
                     * 2：判断是否是主节点,是主节点则执行主节点任务，不是主节点则监听自己的前一个节点的删除事件
                     * 3：执行主节点任务，执行完毕客户端断开与服务器的连接，最后threadCompleteLatch.countDown()
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
        //等待线程池中的任务执行完毕，再真正关闭线程池
        executorService.shutdown();
        
        try {
            threadCompleteLatch.await();
            System.out.println("All thread finished");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}