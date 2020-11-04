package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DistrictWithMostTreesMapper extends Mapper<Object, Text, Text, FloatWritable> {

    private Text lineText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer line = new StringTokenizer(value.toString(), "\r\n");
        String regex = "^[0-9.]+.*$";
        Pattern p = Pattern.compile(regex);
        Matcher m;
        while(line.hasMoreTokens()) {
            lineText.set(line.nextToken());
            String[] tmp = lineText.toString().split(";", -1);
            Text word = new Text(tmp[3]);
            Text height = new Text(tmp[6]);
            m = p.matcher(height.toString());
            if(m.matches()){
                context.write(word, new FloatWritable(Float.parseFloat(height.toString())));
            }
        }
    }
}
