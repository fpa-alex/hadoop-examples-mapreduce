package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TreesBySpeciesMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text lineText = new Text();
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer line = new StringTokenizer(value.toString(), "\r\n");

      while(line.hasMoreTokens()) {
            lineText.set(line.nextToken());
            String[] tmp = lineText.toString().split(";", -1);
            Text word = new Text(tmp[3]);
            context.write(word, one);
        }
    }
}
