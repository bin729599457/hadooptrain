package com.imooc.hadoop.mapreduce.project;

import com.imooc.hadoop.mapreduce.WordCountApp;
import com.kumkee.userAgent.UserAgentParser;
import com.kumkee.userAgent.UserAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 使用MapReduce ：统计浏览器访问次数
 */
public class LogApp {

    /**
     * Map：读取输入的文件
     */
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

        LongWritable one=new LongWritable(1);
        private UserAgentParser userAgentParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            userAgentParser=new UserAgentParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //接收到的每一行数据,其实就是一行日志信息
            String line=value.toString();

            String source=line.substring(getCharacterPosition(line,"\"",5)+1);
            UserAgent agent = userAgentParser.parse(source);
            String browser = agent.getBrowser();
            String engine=agent.getEngine();

            //通过上下文把map的处理结果输出
            context.write(new Text(browser),one);
            context.write(new Text(engine),one);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            userAgentParser=null;
        }
    }

    /**
     * 获取指定字符串中指定标识符的字符串出现的索引的位置
     * @param value
     * @param operator
     * @param index
     * @return
     */
    private static int getCharacterPosition(String value,String operator,int index){


        Matcher matcher= Pattern.compile(operator).matcher(value);
        int mIdx=0;
        while (matcher.find()){
            mIdx++;

            if(mIdx==index){
                break;
            }
        }
        return matcher.start();
    }

    /**
     * Reduce：归并操作
     */
    public static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long sum=0;
            for(LongWritable value:values){
                //求key出现的次数总和
                sum+=value.get();
            }
            //最终统计结果的输出
            context.write(key,new LongWritable(sum));

        }
    }

    /**
     * 定义Driver: 封装了MapReduce的作业的所有信息
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //创建Configuration
        Configuration configuration=new Configuration();

        //准备清理已存在的输出目录
        Path outputPath=new Path(args[1]);
        FileSystem fileSystem=FileSystem.get(configuration);
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath,true);
            System.out.println("output file exists,but it has deleted");
        }

        //创建job(作业)
        Job job=Job.getInstance(configuration,"LogApp");

        //设置job的处理类
        job.setJarByClass(LogApp.class);

        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        //设置map相关的参数
        job.setMapperClass(LogApp.MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce相关参数
        job.setReducerClass(LogApp.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);

    }

}
