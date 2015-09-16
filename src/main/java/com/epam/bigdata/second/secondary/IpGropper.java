package com.epam.bigdata.second.secondary;

import com.epam.bigdata.second.model.IpWrittableComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IpGropper extends WritableComparator {
    protected IpGropper() {
        super(IpWrittableComparable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        IpWrittableComparable ip1 = (IpWrittableComparable) w1;
        return ip1.compareTo(w2);
    }
}
