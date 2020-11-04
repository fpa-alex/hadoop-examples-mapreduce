package com.opstty;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class TreesAgesEntity implements Writable {

    private Text tree;

    private Date plantationYear;

    public TreesAgesEntity(Text tree, Date plantationYear){
        this.tree = tree;
        this.plantationYear = plantationYear;
    }

    public Text getTree() {
        return tree;
    }

    public void setTree(Text tree) {
        this.tree = tree;
    }

    public Date getPlantationYear() {
        return plantationYear;
    }

    public void setPlantationYear(Date plantationYear) {
        this.plantationYear = plantationYear;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
    
    }
}
