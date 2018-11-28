package com.km.sogou;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

public class KeyWordCount {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            StringTokenizer tokenizerArticle = new StringTokenizer(line,"\n");
            while(tokenizerArticle.hasMoreElements()){
                StringTokenizer tokenizerline=new StringTokenizer(tokenizerArticle.nextToken());
                String s1=tokenizerline.nextToken();
                String s2=tokenizerline.nextToken();
                String s3=tokenizerline.nextToken();
                Text newline=new Text(s3);
                context.write(newline,one);
            }
        }
    }
    public static class Reduce extends Reducer<Text,IntWritable,Text,LongWritable>{
        private LongWritable result=new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for (IntWritable val: values){
                count+=val.get();
            }
            result.set(count);
            context.write(key,result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();

        String inputpath="hdfs://192.168.145.128:9000/clean_same_out";
        String outputpath="hdfs://192.168.145.128:9000/key_out";
        Job job=Job.getInstance(conf,"key word count");

        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
        if (fs.exists(new Path(inputpath))) {
            fs.delete(new Path(outputpath), true);
        }

        job.setReducerClass(KeyWordCount.Reduce.class);
        job.setMapperClass(KeyWordCount.Map.class);
        job.setCombinerClass(KeyWordCount.Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现
        job.setInputFormatClass(TextInputFormat.class);

        // 提供一个RecordWriter的实现，负责数据输出
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputpath));
        FileOutputFormat.setOutputPath(job, new Path(outputpath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
