package com.lyf.hdfs.clientapi;

import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Test;

import java.io.IOException;

/**
 * @author liyunfei
 */
public class YarnApiTests {
    @Test
    public void test(){
        //YarnClient yarnClient = new YarnClientImpl();
        YarnClient yarnClient = YarnClient.createYarnClient();
        try {
            yarnClient.getApplications();
            
        } catch (YarnException | IOException e) {
            e.printStackTrace();
        }
    }
}
