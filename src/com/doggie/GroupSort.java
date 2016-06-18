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
    public static class TwoFieldKey implements WritableComparable<TwoFieldKey>{//map输出
        //定义两个属性，index商品编号，content商品内容（内容包含名称和价格，名称类型不应该为Int思考？）
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
                //比较高位，index大就是大的，就好像比大小里面的百位十位，高位大的就大，最高决定权
                return 1;
            }else if (this.getIndex() < other.getIndex()){
                return -1;
            }else{
                //index相等，就是商品编号相等比较以下属性
                return this.getContent() == other.getContent() ? 0
                        :(this.getContent() > other.getContent() ? 1 : -1);//负数比任何价格数都要小
                //大时返回1，同一组内比较是按价格方式对比
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
            return key.getIndex() % numPartitions;//取模操作
        }
    }

    //判断两个对象是否使用一个reduce调用
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
        //map输入<行号，具体内容>思考：如何把map的输出对应到TwoFieldKey
        public void map(Object key,Text value,Context context)
        //context获取当前split
                throws IOException,InterruptedException{
            //强制类型转换，获取文件名
            String fileName = ((FileSplit)context.getInputSplit()).getPath().toString();
            String valueString= value.toString();
            String[] items=valueString.split(" ");//输入文件按空格划分

            TwoFieldKey outputKey = null;
            Text outputValue;

            if(fileName.contains("price")){//1 100
                outputKey = new TwoFieldKey(Integer.parseInt(items[0]), Integer.parseInt(items[1]));
                outputValue = new Text(items[1]);//数据冗余
            }else{//shoes 1
                outputKey = new TwoFieldKey(Integer.parseInt(items[1]),-1);
                //-1是比任何价格都要小的一个记录
                outputValue = new Text("name" + items[0]);
            }
            context.write(outputKey,outputValue);
        }
    }


    public static class reducer extends Reducer<TwoFieldKey,Text,Text,Text> {
        public void reduce(TwoFieldKey key, Iterable<Text>values, Context context)
                throws IOException, InterruptedException {
            Text itemName = null;//第一次进来
            //TreeSet<LongWritable> queue = new TreeSet<LongWritable>();

            for(Text val: values){
                if(itemName == null){
                    itemName = new Text(val);//名称
                    continue;
                }
                //
                context.write(itemName,val);//有一定buffer内存，最终写到hdfs
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
        Job job = new Job(conf, "homework");//读入参数，new一个job
        job.setInputFormatClass(TextInputFormat.class);
        job.setJarByClass(GroupSort.class);//完成商品名称以及价格的join，类似与数据库中的join操作
        //job.setJarByClass(Homework.class);
        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);
        job.setMapOutputKeyClass(TwoFieldKey.class);
        //自定义一个key,按照商品编号，价格/名称，价格和名称同样参与map到reduce之间排序的动作
        //job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setPartitionerClass(PartByIndexPartitioner.class);
        //自定义partitioner,只有商品编号参与,保证同一个index在同一个reduce或者同一partition
        job.setGroupingComparatorClass(IndexGroupingComparator.class);
        /*自定义GroupingComparator,每一个商品的记录都分到了一个
           group当中,在group中再去做对比时用GroupingComparator.class
         */
        //job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(2);
        /*如果设置为1，在一个reduce中没有什么partition好考虑的；为了验证我们的parttitioner
        确实书写没问题reduce number设置为一个大于等于2的值，也可以设置为3,4..其它值*/
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
