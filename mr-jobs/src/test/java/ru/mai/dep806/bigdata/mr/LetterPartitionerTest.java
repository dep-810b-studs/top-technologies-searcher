package ru.mai.dep806.bigdata.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;

import static org.junit.Assert.*;

/**
 * Created by EUGENEL on 22.10.2016.
 */
public class LetterPartitionerTest {
    @org.junit.Test
    public void getPartition() throws Exception {
        WordCount.LetterPartitioner partitioner = new WordCount.LetterPartitioner();

        Assert.assertEquals(0, partitioner.getPartition(new Text("\""), new IntWritable(1), 5));
        Assert.assertEquals(1, partitioner.getPartition(new Text("ABC"), new IntWritable(1), 5));
        Assert.assertEquals(4, partitioner.getPartition(new Text("ZZ9"), new IntWritable(1), 5));
        Assert.assertEquals(1, partitioner.getPartition(new Text("абв"), new IntWritable(1), 5));
        Assert.assertEquals(4, partitioner.getPartition(new Text("яяя"), new IntWritable(1), 5));
    }

}