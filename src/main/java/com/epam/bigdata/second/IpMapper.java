package com.epam.bigdata.second;

import com.epam.bigdata.second.model.IpWrittableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.log4j.Logger;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IpMapper extends Mapper<LongWritable, Text, IpWrittableComparable, NullWritable> {
    private static final Pattern IP_PATTERN = Pattern.compile("^ip[\\d]+");
    private static final Pattern BYTES_PATTERN = Pattern.compile("\\d{3}+ \\d+");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString();

        Matcher ipMatcher = IP_PATTERN.matcher(row);
        String ip = "";
        while (ipMatcher.find()) {
            ip = ipMatcher.group(0);
        }
        Matcher bytesMatcher = BYTES_PATTERN.matcher(row);

        Integer bytes = 0;
        while (bytesMatcher.find()) {
            bytes = Integer.parseInt(bytesMatcher.group(0).split(" ")[1]);
        }

        context.write(new IpWrittableComparable(ip, bytes), NullWritable.get());
    }
}

