package com.epam.bigdata.second;

import com.epam.bigdata.second.model.IpWrittableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IpReducer extends Reducer<IpWrittableComparable, NullWritable, IpWrittableComparable, NullWritable> {

    @Override
    public void reduce(IpWrittableComparable key, Iterable<NullWritable> values,
                       Context context)
            throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
