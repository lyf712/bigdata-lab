package com.nju.cloudcomputecourse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 端：MongoDB、MySQL数据库、HadoopFile、本地textFile、Kafka流
 * parallelize()
 * textFile(path)
 * hadoopFile(path)
 * sequenceFile(path)
 * objectFile(path)
 * binaryFiles(path)
 * ----------------------
 * saveAsTextFile(path)
 * saveAsSequenceFile(path)
 * saveAsObjectFile(path)
 * saveAsHadoopFile(path)
 *-----------------------
 * 源 → RDD
 * RDD → 目标
 *-----------------------
 * 理解 RDD机制、存储、执行器和调度器、shuffle
 *
 * @authorliyunfei
 * @date2022/11/22
 **/
public class JavaSparkBaseOp {
    static final String PATH = "E:\\JavaProjects\\CourseProjects\\bigdata-lab\\base-learn\\bigdata-spark\\src\\main\\resources\\spark\\test_sort.txt";
    /**
     * 算子 API 对比--Stream计算，map,sort,reduce
     * 启动类测试
     * @param args
     */
    public static void main(String[] args) {
              //spark_core
              System.setProperty("HADOOP_USER_NAME","root");
              //Caused by: java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset.
              SparkConf sparkConf = new SparkConf();
              sparkConf.setMaster("local[1]");
              sparkConf.setAppName("test");
              JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

              //javaSparkContext.
              // Conifg 配置基础信息
              List<Tuple2<String,Object>> list = new ArrayList<>();
              //Tuple2<String,Object> t1 = new Tuple2<>("hello",1);// Tuplenum ,几元祖 -- 行
              for(int i=0;i<1000;i++){// 随机生成元祖列表
                 list.add(new Tuple2<>("key"+i,new Random().nextInt(1000)));
              }
              // 数据来源 → 生成RDD
              JavaPairRDD<String,Object> javaPairRDD = javaSparkContext.parallelizePairs(list);

              // 由RDD进行计算
              List<Tuple2<Object,String>> list1 = javaPairRDD.mapToPair(new PairFunction<Tuple2<String, Object>, Object, String>() {
                  @Override
                  public Tuple2<Object, String> call(Tuple2<String, Object> stringObjectTuple2) throws Exception {
                      return new Tuple2<>(stringObjectTuple2._2,stringObjectTuple2._1);
                  }
              }).sortByKey(false).take(10);

              list1.forEach(u->{
                  System.out.println(u._2+":"+u._1);
              });

              javaSparkContext.parallelizePairs(list1).saveAsTextFile(PATH);

       }
}
