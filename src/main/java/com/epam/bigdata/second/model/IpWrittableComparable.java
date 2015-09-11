package com.epam.bigdata.second.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IpWrittableComparable implements WritableComparable {
    private double average;
    private int count;

    public IpWrittableComparable(double average, int count) {
        this.average = average;
        this.count = count;
    }

    public IpWrittableComparable() {
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public int compareTo(Object o) {
        IpWrittableComparable comparable = (IpWrittableComparable) o;
        int averageResult = Double.compare(average, comparable.getAverage());
        int countResult = Integer.compare(count, comparable.getCount());
        return Integer.compare(averageResult, countResult);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(average);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.average = dataInput.readDouble();
        this.count = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IpWrittableComparable)) {
            return false;
        }

        IpWrittableComparable that = (IpWrittableComparable) o;

        if (Double.compare(that.average, average) != 0) {
            return false;
        }
        if (count != that.count) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(average);
        result = (int) (temp ^ (temp >>> 32));
        result = 31 * result + count;
        return result;
    }
}
