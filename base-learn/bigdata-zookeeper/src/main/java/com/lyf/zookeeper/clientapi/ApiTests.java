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

/**
 * @author liyunfei
 */
public class ApiTests {
    //
    private final static String URL = "hadoop1:2181,hadoop2:2181,hadoop3:281";
    private ZooKeeper zooKeeper;
    //@Test
    @Before
    public void testCon() throws IOException {
        //ZooKeeper
        zooKeeper = new ZooKeeper(URL, 2000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                   System.out.println(watchedEvent);
            }
        });
    }
    @Test
    public void testBaseOp(){
        System.out.println("state:"+zooKeeper.getState().isAlive());
        String path = "/testClientApi";
        try {
            System.out.println("create node");
            System.out.println(zooKeeper.create(path,"ok".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL));
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("get node");
        try {
            System.out.println(new String(zooKeeper.getData(path, false, new Stat()))); //Arrays.toString(zooKeeper.getData(path, false, new Stat()))
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
