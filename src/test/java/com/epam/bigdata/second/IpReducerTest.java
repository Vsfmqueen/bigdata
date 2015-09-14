package com.epam.bigdata.second;

import com.epam.bigdata.second.model.IpWrittableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class IpReducerTest {
    ReduceDriver<Text, IpWrittableComparable, Text, Text> reduceDriver;

    @Before
    public void setUp() {
        IpReducer reducer = new IpReducer();
        reduceDriver = new ReduceDriver();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        List<IpWrittableComparable> values = Arrays.asList(new IpWrittableComparable(12.5, 15));
        reduceDriver.withInput(new Text("ip1"), values);
        reduceDriver.withOutput(new Text("ip1"), new Text("12.5,15"));
        reduceDriver.runTest();
    }
}
