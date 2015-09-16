package com.epam.bigdata.second.secondary;

import com.epam.bigdata.second.model.IpWrittableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class IpPartitioner extends Partitioner<IpWrittableComparable, NullWritable> {

    @Override
    public int getPartition(IpWrittableComparable ipWrittableComparable, NullWritable nullWritable, int numPartitions) {
        return Math.abs(ipWrittableComparable.getIp().hashCode()) % numPartitions;
    }
}
