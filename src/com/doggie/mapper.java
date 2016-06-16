package com.doggie;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by root on 5/25/16.
 */
public class mapper extends Mapper<Object,Text,LongWritable,Text> {
    public void map(Object key,Text value,Context context)
        throws IOException,InterruptedException{
        String fileName = ((FileSplit)context.getInputSplit()).getPath().toString();
        String valueString= value.toString();
        String[] items=valueString.split(" ");

        LongWritable outputKey = null;
        Text outputValue=null;

        if(fileName.contains("price")){
            outputKey = new LongWritable(Long.valueOf(items[0]));
            outputValue = new Text(items[1]);
        }else{
            outputKey = new LongWritable(Long.valueOf(items[1]));
            outputValue = new Text("name" + items[0]);
        }
        context.write(outputKey,outputValue);
    }
}
