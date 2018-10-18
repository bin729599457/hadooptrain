package com.imooc.hadoop.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 使用MapReduce开发WordCount应用程序
 */
public class WordCountApp {

    /**
     * Map：读取输入的文件
     */
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }

    /**
     * Reduce：归并操作
     */
    public static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }
}
