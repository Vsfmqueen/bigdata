package com.epam.bigdata.second.secondary;

import com.epam.bigdata.second.model.IpWrittableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IpComparator extends WritableComparator {
    protected IpComparator() {
        super(IpWrittableComparable.class, true);
    }

    @Override
    public int compare(Object first, Object second) {
        return ((IpWrittableComparable) first).compareTo(second);
    }
}
