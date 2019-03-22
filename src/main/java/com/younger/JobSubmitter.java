package com.younger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobSubmitter {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 在代码中设置JVM系统参数，用于给job对象来获取访问HDFS的用户身份
        //System.setProperty("HADOOP_USER_NAME", "root");

        Configuration entries = new Configuration();
        // 设置job运行时要访问的默认文件系统
        //entries.set("fs.defaultFS", "hdfs://node01:8020");
        // 设置job提交到哪儿运行
        //entries.set("mapreduce.framework.name","yarn");
        //entries.set("yarn.resourcemanager.hostname","node01");
        // 如果要从Window系统上运行这job提交客户端程序，则需要加这个跨平台提交的参数
        //entries.set("mapreduce.app-submission.cross-platform","true");

        Job job = Job.getInstance(entries);
        // 设置job名称
        job.setJobName("WordCount");
        // 封装参数：jar包所在的位置
        //job.setJar("d:/wc.jar");
        job.setJarByClass(JobSubmitter.class);

        // 封装参数：本次job所要调用Mapper实现类，Reducer实现类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 封装参数：本次job的Mapper实现类、Reducer实现类产生的结果数据key、value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 封装参数：本次job要输入数据集所在路径、输出结果的输出路径
        //FileInputFormat.setInputPaths(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, new Path("F:\\codedata\\mapreducetest1\\test1.txt"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\codedata\\mapreducetest1\\output"));

        // 压缩
        //FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        // 封装参数：自定义reduce task的数量
        job.setNumReduceTasks(2);

        // 提交job给yarn
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:-1);

    }
}
