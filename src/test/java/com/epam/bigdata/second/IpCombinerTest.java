package com.epam.bigdata.second;

import com.epam.bigdata.second.model.IpWrittableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class IpCombinerTest {
    ReduceDriver<Text, IntWritable, Text, IpWrittableComparable> combineDriver;

    @Before
    public void setUp() {
        IpCombiner combiner = new IpCombiner();
        combineDriver = new ReduceDriver();
        combineDriver.setReducer(combiner);
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = Arrays.asList(new IntWritable(1), new IntWritable(2), new IntWritable(3));
        int sum = 6;
        int count = values.size();
        double average = sum / count;

        combineDriver.withInput(new Text("ip1"), values);
        combineDriver.withOutput(new Text("ip1"), new IpWrittableComparable(average, count));
        combineDriver.runTest();
    }
}
