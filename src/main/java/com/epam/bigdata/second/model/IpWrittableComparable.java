package com.epam.bigdata.second.model;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IpWrittableComparable implements WritableComparable {
    private String ip;
    private int count;

    public IpWrittableComparable(String ip, int count) {
        this.ip = ip;
        this.count = count;
    }

    public IpWrittableComparable() {
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public int compareTo(Object o) {
        IpWrittableComparable firstComparable = (IpWrittableComparable) o;
        int ipResult = this.getIp().compareTo(firstComparable.getIp());

        if (ipResult == 0) {
            return Integer.compare(this.getCount(), firstComparable.getCount());
        }

        return ipResult;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ip);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.ip = dataInput.readUTF();
        this.count = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString() {
        return ip + "," + count;
    }
}
