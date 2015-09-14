package com.epam.bigdata.second;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class IpDriver extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(IpDriver.class);

    public static void main(String... args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new IpDriver(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf);
        LOG.info("IP Job has started");

        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(IpMapper.class);
     //   job.setCombinerClass(IpCombiner.class);
        job.setReducerClass(IpCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean isWaitForCompletion = job.waitForCompletion(true);
        LOG.info("IP Job has ended");
        return isWaitForCompletion ? 0 : 1;
    }
}
