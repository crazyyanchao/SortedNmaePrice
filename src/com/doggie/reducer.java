package com.doggie;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.TreeSet;

/**
 * Created by root on 5/25/16.
 */
public class reducer extends Reducer<LongWritable,Text,Text,LongWritable> {
    public void reduce(LongWritable key, Iterable<Text>values, Context context)
            throws IOException, InterruptedException {

        Text itemName = null;
        TreeSet<LongWritable> queue = new TreeSet<LongWritable>();

        for (Text val : values){
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
        }
    }
}




















