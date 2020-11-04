package com.opstty.reducer;

import com.opstty.TreeDetailsWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    private TreeDetailsWritable minYearDetails;

    public void reduce(Text key, Iterable<TreeDetailsWritable> values, Context context)
            throws IOException, InterruptedException {

        for (TreeDetailsWritable val : values) {
            if(null == minYearDetails
                    || minYearDetails.getPlantationYear().get() > val.getPlantationYear().get())
                minYearDetails = val;
        }
        result.set(minYearDetails.getDistrict().get());
        context.write(minYearDetails.getWord(), result);
    }
}