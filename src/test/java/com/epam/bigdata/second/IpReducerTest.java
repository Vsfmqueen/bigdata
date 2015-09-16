package com.epam.bigdata.second;

import com.epam.bigdata.second.model.IpWrittableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class IpReducerTest {
    ReduceDriver<IpWrittableComparable, NullWritable, IpWrittableComparable, NullWritable> reduceDriver;

    @Before
    public void setUp() {
        IpReducer reducer = new IpReducer();
        reduceDriver = new ReduceDriver();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        List<NullWritable> values = Arrays.asList(NullWritable.get());
        reduceDriver.withInput(new IpWrittableComparable("ip1", 15), values);
        reduceDriver.withOutput(new IpWrittableComparable("ip1", 15), NullWritable.get());
        reduceDriver.runTest();
    }

}
