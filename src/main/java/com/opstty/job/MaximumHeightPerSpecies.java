package com.opstty.job;

import com.opstty.mapper.DistrictMapper;
import com.opstty.mapper.MaximumHeightPerSpeciesMapper;
import com.opstty.reducer.DistrictReducer;
import com.opstty.reducer.MaximumHeightPerSpeciesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MaximumHeightPerSpecies {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: maximum height <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "maximumheight");
        job.setJarByClass(MaximumHeightPerSpecies.class);
        job.setMapperClass(MaximumHeightPerSpeciesMapper.class);
        job.setCombinerClass(MaximumHeightPerSpeciesReducer.class);
        job.setReducerClass(MaximumHeightPerSpeciesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    //yarn jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar districtcount dataset.csv datasetDistrictCount
    //hdfs dfs -cat datasetDistrictCount/part-r-00000
    //hdfs dfs -rm -r datasetDistrictCount

}