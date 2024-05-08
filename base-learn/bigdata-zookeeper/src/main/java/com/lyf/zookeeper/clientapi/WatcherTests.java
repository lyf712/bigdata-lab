package com.lyf.zookeeper.clientapi;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * https://developer.aliyun.com/article/838030
 * https://wenku.baidu.com/view/91aa82df0142a8956bec0975f46527d3240ca6ff.html?_wkts_=1670398909441&bdQuery=Zookeeper+Watcher
 *
 * @authorliyunfei
 * @date2022/12/7
 **/
public class WatcherTests {
    private final static String LOCAL_URL = "localhost:2181";
    // 集群情况下 session loss,集群间的心跳机制超时》》？需要设置调整
    private ZooKeeper zooKeeper1;
    private ZooKeeper zooKeeper2;

    //@Test
    @Before
    public void testCon() throws IOException {
        //ZooKeeper ,超时机制设置长一点
//        WatchedEvent watchedEvent = new WatchedEvent();
//        Watcher watcher = new Watcher() {
//            @Override
//            public void process(WatchedEvent watchedEvent) {
//
//            }
//        };
        zooKeeper1 = new ZooKeeper(LOCAL_URL, 10000,
                watchedEvent -> {
                    if(watchedEvent.getPath().equals("/practice1")){
                        System.out.println("zk1:"+watchedEvent);
                    }
                }
        );

        zooKeeper2 = new ZooKeeper(LOCAL_URL, 10000,
                watchedEvent -> {
                    if(watchedEvent.getPath().equals("/practice1")){
                        System.out.println("zk2:"+watchedEvent);
                    }
                }
        );
    }

    @Test
    public void test1() throws InterruptedException, KeeperException {
        zooKeeper1.create("/practice2","ok".getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.EPHEMERAL);
        zooKeeper2.getData("/practice2", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent);
            }
        },new Stat());

        TimeUnit.SECONDS.sleep(1);
    }

}
