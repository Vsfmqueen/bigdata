package com.epam.bigdata.second;

import com.epam.bigdata.second.model.IpWrittableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class IpDriverTest {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    //  ReduceDriver<Text, IntWritable, Text, IpWrittableComparable> combineDriver;
    ReduceDriver<Text, IpWrittableComparable, Text, Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        mapDriver = MapDriver.newMapDriver(new IpMapper());
        // combineDriver = ReduceDriver.newReduceDriver(new IpCombiner());
        reduceDriver = ReduceDriver.newReduceDriver(new IpReducer());
        mapReduceDriver = new MapReduceDriver();
    //    mapReduceDriver.setCombiner(reduceDriver);

    }

    @Test
    public void testMapper() throws IOException {
        //    new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, Text>().withMapper(new MapDriver<LongWritable, Text, Text, IntWritable>())
    }
}
