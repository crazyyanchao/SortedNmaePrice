package com.doggie;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.TreeSet;

/**
 * Created by root on 16-6-17.
 */

public class GroupSort {
    public static class TwoFieldKey implements WritableComparable<TwoFieldKey>{
        private IntWritable index;
        private IntWritable content;

        public TwoFieldKey(){
            index = new IntWritable();
            content = new IntWritable();
        }

        public TwoFieldKey(int index,int content){
            this.index = new IntWritable(index);
            this.content = new IntWritable(content);
        }

        public int getIndex(){
            return index.get();
        }

        public int getContent(){
            return content.get();
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            index.readFields(in);
            content.readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            index.write(out);
            content.write(out);
        }

        @Override
        public int compareTo(TwoFieldKey other) {
            if (this.getIndex() > other.getIndex()){
                return 1;
            }else if (this.getIndex() < other.getIndex()){
                return -1;
            }else{
                return this.getContent() == other.getContent() ? 0
                        :(this.getContent() > other.getContent() ? 1 : -1);
            }
        }

        @Override
        public boolean equals(Object right) {
            if (right instanceof TwoFieldKey){
                TwoFieldKey r = (TwoFieldKey) right;
                return r.index.get() == index.get();
            }else{
                return false;
            }
        }

        @Override
        public String toString() {
            return "index" + index + "Content" + content;
        }
    }



    public static class PartByIndexPartitioner extends Partitioner<TwoFieldKey,Text>{
        @Override
        public int getPartition(TwoFieldKey key, Text value, int numPartitions) {
            return key.getIndex() % numPartitions;
        }
    }

    //
    public static class IndexGroupingComparator extends WritableComparator{
        protected IndexGroupingComparator() {
            super(TwoFieldKey.class,true);
        }

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            int IIndex = ((TwoFieldKey) o1).getIndex();
            int rIndex = ((TwoFieldKey) o2).getIndex();
            return IIndex == rIndex ? 0 :(IIndex < rIndex ? -1 : 1);
        }
    }


    public static class mapper extends Mapper<Object,Text,TwoFieldKey,Text> {
        public void map(Object key,Text value,Context context)
                throws IOException,InterruptedException{
            String fileName = ((FileSplit)context.getInputSplit()).getPath().toString();
            String valueString= value.toString();
            String[] items=valueString.split(" ");

            TwoFieldKey outputKey = null;
            Text outputValue;

            if(fileName.contains("price")){
                outputKey = new TwoFieldKey(Integer.parseInt(items[0]), Integer.parseInt(items[1]));
                outputValue = new Text(items[1]);
            }else{
                outputKey = new TwoFieldKey(Integer.parseInt(items[1]),-1);//
                outputValue = new Text("name" + items[0]);
            }
            context.write(outputKey,outputValue);
        }
    }


    public static class reducer extends Reducer<TwoFieldKey,Text,Text,Text> {
        public void reduce(TwoFieldKey key, Iterable<Text>values, Context context)
                throws IOException, InterruptedException {
            Text itemName = null;
            //TreeSet<LongWritable> queue = new TreeSet<LongWritable>();

            for(Text val: values){
                if(itemName == null){
                    itemName = new Text(val);
                    continue;
                }
                //
                context.write(itemName,val);
            }
            /*for (Text val : values){
                if(val.toString().startsWith("name")){
                    String realName = val.toString().substring(4);
                    itemName = new Text(realName);
                }else{
                    LongWritable price = new LongWritable(Long.valueOf(val.toString()));//如果是价格则进入队列
                    queue.add(price);
                }
            }
            for (LongWritable val : queue) {//遍历queue(这是一个耗费内存的解法)
                context.write(itemName, val);
            }*/
        }
    }



    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: homework");
            System.exit(2);
        }
        //conf.setInt("mapred.task.timeout",100);
        Job job = new Job(conf, "homework");
        job.setInputFormatClass(TextInputFormat.class);
        job.setJarByClass(GroupSort.class);
        //job.setJarByClass(Homework.class);
        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);
        job.setMapOutputKeyClass(TwoFieldKey.class);
        //job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setPartitionerClass(PartByIndexPartitioner.class);
        job.setGroupingComparatorClass(IndexGroupingComparator.class);
        //job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(2);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
