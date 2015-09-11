package com.epam.bigdata.second;

import com.epam.bigdata.second.model.IpWrittableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IpReducer extends Reducer<Text, IpWrittableComparable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<IpWrittableComparable> values,
                       Context context)
            throws IOException, InterruptedException {
        String outputValue = "";
        for (IpWrittableComparable value : values) {
            outputValue = value.getAverage() + "," + value.getCount();
        }
        context.write(key, new Text(outputValue));
    }
}
