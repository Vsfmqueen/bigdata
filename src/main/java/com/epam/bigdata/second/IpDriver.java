package com.epam.bigdata.second;

import com.epam.bigdata.second.model.IpWrittableComparable;
import com.epam.bigdata.second.secondary.IpComparator;
import com.epam.bigdata.second.secondary.IpGropper;
import com.epam.bigdata.second.secondary.IpPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.CounterGroup;
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

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);

        job.setMapperClass(IpMapper.class);
        job.setReducerClass(IpReducer.class);

        job.setGroupingComparatorClass(IpGropper.class);
        job.setSortComparatorClass(IpComparator.class);
        job.setPartitionerClass(IpPartitioner.class);

        job.setMapOutputKeyClass(IpWrittableComparable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(IpWrittableComparable.class);
        job.setOutputValueClass(NullWritable.class);

        boolean isWaitForCompletion = job.waitForCompletion(true);

        LOG.info("IP Job has ended");
        return isWaitForCompletion ? 0 : 1;
    }
}
