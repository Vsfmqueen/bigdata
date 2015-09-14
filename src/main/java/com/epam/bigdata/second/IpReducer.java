package com.epam.bigdata.second;

import com.epam.bigdata.second.model.IpWrittableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class IpReducer extends Reducer<Text, IpWrittableComparable, Text, Text> {
    private static final Logger LOG = Logger.getLogger(IpReducer.class);
    @Override
    public void reduce(Text key, Iterable<IpWrittableComparable> values,
                       Context context)
            throws IOException, InterruptedException {
        LOG.info("Reducing for ip = " + key + "has started!");
        String outputValue = "";
        for (IpWrittableComparable value : values) {
            outputValue = value.getAverage() + "," + value.getCount();
        }
        context.write(key, new Text(outputValue));
        LOG.info("Reducing for ip = " + key + "has ended! Value = " + outputValue);
    }
}
