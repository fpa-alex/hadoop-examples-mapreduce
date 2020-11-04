package com.opstty.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistrictWithMostTreesReducer  extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {

        float maxHeight = 0;
        for (FloatWritable val : values) {
            if(val.get() >= maxHeight){
                maxHeight = val.get();
            }
        }
        result.set(maxHeight);
        context.write(key, result);
    }

}
