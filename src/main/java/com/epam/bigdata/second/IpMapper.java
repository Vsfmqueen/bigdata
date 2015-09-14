package com.epam.bigdata.second;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.log4j.Logger;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IpMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Pattern IP_PATTERN = Pattern.compile("^ip[\\d]+");
    private static final Pattern BYTES_PATTERN = Pattern.compile("\\d{3}+ \\d+");
    private static final Logger LOG = Logger.getLogger(IpMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString();

        LOG.info("Row = " + row);

        Matcher ipMatcher = IP_PATTERN.matcher(row);
        String ip = "";
        while (ipMatcher.find()) {
            ip = ipMatcher.group(0);
            LOG.info("IP = " + ip);
        }
        Matcher bytesMatcher = BYTES_PATTERN.matcher(row);
        Integer bytes = 0;
        while (bytesMatcher.find()) {
            bytes = Integer.parseInt(bytesMatcher.group(0).split(" ")[1]);
            LOG.info("BYTES = " + bytes);
        }

        context.write(new Text(ip), new IntWritable(bytes));
        calculateBrowsers(row, context);
        LOG.info("Mapping has ended. IP = " + ip + " Bytes = " + bytes);
    }

    private void calculateBrowsers(String logLine, Context context) {
        if (logLine.contains("Mozilla")) {
            context.getCounter("browser", "Mozilla").increment(1);
        } else if (logLine.contains("MSIE")) {
            context.getCounter("browser", "MSIE").increment(1);
        } else if (logLine.contains("Opera")) {
            context.getCounter("browser", "Opera").increment(1);
        } else {
            context.getCounter("browser", "Other").increment(1);
        }
    }
}

