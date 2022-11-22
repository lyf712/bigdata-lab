package com.lyf.zookeeper.clientapi;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @author liyunfei
 */
public class ApiTests {
    //
    private final static String URL = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
   // private final static String NJU_URL = "172.19.240.61:2181,172.19.240.178:2181,172.19.240.191:2181";//112.124.24.187

    private final static String NJU_URL = "112.124.24.187:2181";
    // 集群情况下 session loss,集群间的心跳机制超时》》？需要设置调整
    private ZooKeeper zooKeeper;

    //@Test
    @Before
    public void testCon() throws IOException {
        //ZooKeeper ,超时机制设置长一点
        zooKeeper = new ZooKeeper(NJU_URL, 10000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent);
            }
        });
    }

    @Test
    public void testBaseOp() throws InterruptedException, KeeperException {
        System.out.println("state:" + zooKeeper.getState().isAlive());
        String path = "/practice";
        try {
            System.out.println("create node " + path);
            // EPHEMERAL 临时节点
            System.out.println(zooKeeper.create(path, "ok".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL));
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("get node " + path);
        try {
            System.out.println(new String(zooKeeper.getData(path, false, new Stat()))); //Arrays.toString(zooKeeper.getData(path, false, new Stat()))
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        // 测试临时节点
        TimeUnit.SECONDS.sleep(20);
    }

    @Test
    public void testDisdributeLock() throws InterruptedException {
        final String path = "/practice";
        // 10个客户端竞争
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                try {
                    String rs = zooKeeper.create(path, "ok".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    if (path.equals(rs)) {
                        System.out.println(Thread.currentThread().getName() + ":create success");
                    } else {
                        System.out.println(Thread.currentThread().getName() + ":create failed");// KeeperErrorCode = NodeExists for /practice
                    }
                } catch (KeeperException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }, "thread-" + i).start();
        }
        new Thread(()->{
            // 主线程或其他线程监控
            int i=0;
            while (true){
                if(i>10)
                    break;
                try {
                    Stat stat =zooKeeper.exists(path, new Watcher() {
                        @Override
                        public void process(WatchedEvent watchedEvent) {
                            System.out.println(path+"::"+watchedEvent.getState());
                        }
                    });
                    // stat
                    System.out.println(stat.getAversion());
                } catch (KeeperException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                i++;
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        TimeUnit.SECONDS.sleep(10);
    }

}
