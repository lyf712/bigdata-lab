package com.lyf.sample;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @authorliyunfei
 * @date2022/11/6
 **/
public class WorkCount {
    public static class TokenizerMapper extends org.apache.hadoop.mapreduce.Mapper<Object,Text, Text,IntWritable>{
        private Text word = new Text();
        @Override
        protected void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            IntWritable one = new IntWritable(1);
//            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
//            while (stringTokenizer.hasNext()){
//
//            }
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            while (stringTokenizer.hasMoreTokens()){
                  this.word.set(stringTokenizer.nextToken());
                  context.write(this.word,one);
            }

            // super.map(key, value, context);
        }
    }
    public static class IntSumReducer extends org.apache.hadoop.mapreduce.Reducer<Text,IntWritable, Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }
}
