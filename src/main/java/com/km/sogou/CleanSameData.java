package com.km.sogou;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class CleanSameData {
    public static class Map extends Mapper<Object, Text, Text, Text> {
        // 实现map函数
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 将输入的纯文本文件的数据转化成String
            String line = value.toString();
            // 将输入的数据首先按行进行分割
            StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");
            // 分别对每一行进行处理
            int count=0;
            while (tokenizerArticle.hasMoreElements()) {
                // 每行按空格划分
                StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
                String c1 = tokenizerLine.nextToken();// time
                String c2 = tokenizerLine.nextToken();//用户id
                String c3 = tokenizerLine.nextToken(); //查询词
                c3 = c3.substring(1, c3.length() - 1);
                Text newline = new Text(c1 + "    " + c2 + "    " +c3);
                context.write(newline, new Text(""));
            }

        }

    }

    // reduce将输入中的key复制到输出数据的key上，并直接输出
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        // 实现reduce函数
        //reduce接受的是mapper产生的键值对集合，但是mapper已经将相同key的键值对合并了，所以value是一个集合
        //因此这里的value是一个Iterable<Text>可迭代的集合
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            context.write(key, new Text(""));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 设置输入输出文件目录
        String[] ioArgs = new String[] { "hdfs://192.168.145.128:9000/data_in", "hdfs://192.168.145.128:9000/clean_same_out" };
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage:  <in> <out>");
            System.exit(2);
        }
        // 设置一个job
        Job job = Job.getInstance(conf, "clean same data");
        job.setJarByClass(CleanSameData.class);

        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现
        job.setInputFormatClass(TextInputFormat.class);

        // 提供一个RecordWriter的实现，负责数据输出
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
