package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExistingSpeciesMapper extends Mapper<Object, Text, Text, NullWritable> {

    private Text lineText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer line = new StringTokenizer(value.toString(), "\r\n");



        while(line.hasMoreTokens()) {
            lineText.set(line.nextToken());
          String[] tmp = lineText.toString().split(";", -1);
          Text word = new Text(tmp[3]);
          context.write(word, NullWritable.get());
        }
    }

}
