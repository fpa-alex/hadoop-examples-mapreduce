package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DistrictMapper extends Mapper<Object, Text, Text, NullWritable> {

    private Text lineText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer line = new StringTokenizer(value.toString(), "\r\n");
        String regex = "^[0-9]+$";
        Pattern p = Pattern.compile(regex);
        Matcher m;
        while(line.hasMoreTokens()) {
            lineText.set(line.nextToken());
            String[] tmp = lineText.toString().split(";",-1);
            Text word = new Text(tmp[1]);
            m = p.matcher(word.toString());
            if(m.matches()){
                int district = Integer.parseInt(word.toString());
                if(district>=1 && district <=20)
                    context.write(word,NullWritable.get());
            }
        }
    }

}
