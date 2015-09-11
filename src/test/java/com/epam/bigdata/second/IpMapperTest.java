package com.epam.bigdata.second;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class IpMapperTest {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    @Before
    public void setUp() {
        IpMapper mapper = new IpMapper();
        mapDriver = new MapDriver();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text("ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 56928 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        mapDriver.withInput(new LongWritable(1), new Text("ip1 - - [24/Apr/2011:04:18:54 -0400] \"GET /~strabal/grease/photo1/T97-4.jpg HTTP/1.1\" 200 6244 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        mapDriver.withInput(new LongWritable(1), new Text("ip6 - - [24/Apr/2011:04:25:26 -0400] \"GET / HTTP/1.1\" 200 12550 \"-\" \"Baiduspider+(+http://www.baidu.com/search/spider.htm)\""));
        mapDriver.withOutput(new Text("ip1"), new IntWritable(56928));
        mapDriver.withOutput(new Text("ip1"), new IntWritable(6244));
        mapDriver.withOutput(new Text("ip6"), new IntWritable(12550));
        mapDriver.runTest();
    }


}
