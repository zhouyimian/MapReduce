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
import java.net.URISyntaxException;
import java.util.TreeMap;

public class TopKeyword {
    public static final int K=100;
    public static class Map extends Mapper<LongWritable,Text,IntWritable,Text>{
        TreeMap<Integer, String> map = new TreeMap<Integer, String>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if(line.trim().length()>0&&line.indexOf("\t")!=-1){
                String[] arr=line.split("\t");
                map.put(Integer.parseInt(arr[1]),arr[0]);
                if(map.size()>K)
                    map.remove(map.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Integer num:map.keySet()){
                context.write(new IntWritable(num),new Text(map.get(num)));
            }
        }
    }

    public static class reduce extends Reducer<IntWritable,Text,IntWritable,Text>{
        TreeMap<Integer, String> map = new TreeMap<Integer, String>();
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            map.put(key.get(), values.iterator().next().toString());
            if(map.size() > K) {
                map.remove(map.firstKey());
            }
        }
        @Override
        protected void cleanup(Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            for(Integer num : map.keySet()) {
                context.write(new IntWritable(num), new Text(map.get(num)));
            }
        }
    }

    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();

        String inputpath="hdfs://192.168.145.128:9000/key_out";
        String outputpath="hdfs://192.168.145.128:9000/TopK_out";

        FileSystem fs=FileSystem.get(new URI("hdfs://192.168.145.128:9000/key_out"),conf);
        if (fs.exists(new Path(outputpath))) {
            fs.delete(new Path(outputpath), true);
        }
        Job job=Job.getInstance(conf,"TopK sort");

        job.setMapperClass(Map.class);
        job.setReducerClass(reduce.class);

        job.setOutputKeyClass(IntWritable.class);
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
