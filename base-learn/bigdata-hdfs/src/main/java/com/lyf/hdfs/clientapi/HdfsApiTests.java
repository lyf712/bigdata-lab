package com.lyf.hdfs.clientapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

/**
 * @author liyunfei
 */
public class HdfsApiTests {
    private final String HDFS_URL = "hdfs://192.168.75.100:9002";//hadoop1
    private final String SCR_PATH = "E:\\JavaProjects\\CourseProjects\\bigdata-lab\\base-learn\\bigdata-hdfs\\src\\main\\resources\\files\\test_data.json";
    private final String DST_PATH = "/test";
    
    @Before
    public void initLog() {
        FileInputStream fileInputStream = null;
        System.out.println("init log");
        try {
            Properties properties = new Properties();
            fileInputStream = new FileInputStream("src/main/resources/log4j.properties");
            properties.load(fileInputStream);
            PropertyConfigurator.configure(properties);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    @Test
    public void testURL(){
        try {
            new URL(HDFS_URL);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testCopyFile(){
        System.setProperty("HADOOP_USER_NAME","root");
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",HDFS_URL);
        FileSystem fs= null;
        
        try {
            fs = FileSystem.get(conf);
            Path src=new Path(SCR_PATH);
            Path dst=new Path(DST_PATH);
            System.out.println("connected");
            // ??????????????????????????????????????????????????????????????????
            // time out ????????????????????????????????????,?????????????????????
            // ??????????????????file???????????????????????????????????????????????????????????????HDFS????????????????????????10??????+30???
            // ???????????? 0.0.0.0 ????????? localhost???0.0.0.0???127.0.0.1
            //
            if(!fs.exists(new Path("/t2"))){
                System.out.println("doest exists");
                fs.mkdirs(new Path("/t2"));
            }
            System.out.println("ok!");
           // fs.copyFromLocalFile(src,dst);
            //fs.listFiles(dst,true);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                assert fs != null;
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
