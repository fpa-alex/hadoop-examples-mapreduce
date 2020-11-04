package com.opstty;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TreeDetailsWritable implements Writable {

    private IntWritable plantationYear;

    private IntWritable district;

    private Text word;

    public TreeDetailsWritable() {
        this.plantationYear = new IntWritable(0);
        this.district = new IntWritable(0);
        this.word = new Text();
    }

    public TreeDetailsWritable(IntWritable plantationYear, IntWritable district, Text word) {
        this.plantationYear = plantationYear;
        this.district = district;
        this.word = word;
    }

    public IntWritable getPlantationYear() {
        return plantationYear;
    }

    public void setPlantationYear(IntWritable plantationYear) {
        this.plantationYear = plantationYear;
    }

    public IntWritable getDistrict() {
        return district;
    }

    public void setDistrict(IntWritable district) {
        this.district = district;
    }

    public Text getWord() {
        return word;
    }

    public void setWord(Text word) {
        this.word = word;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        plantationYear.write(dataOutput);
        district.write(dataOutput);
        word.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        plantationYear.readFields(dataInput);
        district.readFields(dataInput);
        word.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "TreeDetailsWritable{" +
                "plantationYear=" + plantationYear +
                ", district=" + district +
                ", word=" + word +
                '}';
    }
}
