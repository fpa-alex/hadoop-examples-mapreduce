package com.opstty.mapper;

import com.opstty.TreeDetailsWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OldestTreeMapper extends Mapper<Object, Text, Text, TreeDetailsWritable> {

    private Text lineText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer line = new StringTokenizer(value.toString(), "\r\n");
        String regex = "^[0-9]+$";
        Pattern p = Pattern.compile(regex);
        Matcher m;
        Matcher n;

        while (line.hasMoreTokens()) {
            lineText.set(line.nextToken());
            String[] tmp = lineText.toString().split(";", -1);
            Text word = new Text(tmp[3]);
            Text district = new Text(tmp[1]);
            Text year = new Text(tmp[5]);
            m = p.matcher(district.toString());
            n = p.matcher(year.toString());

            if(m.matches() && n.matches()){
                context.write(year,
                        new TreeDetailsWritable(
                                new IntWritable(Integer.parseInt(tmp[5])),
                                new IntWritable(Integer.parseInt(tmp[1])), word));

            }
        }
    }
}