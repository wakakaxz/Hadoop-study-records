package com.xz.mapreduce.partitionerAndwritableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner2 extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {

        String phone = text.toString();
        String pre = phone.substring(0, 3);

        if ("136".equals(pre)) {
            return 0;
        } else if ("137".equals(pre)) {
            return 1;
        } else if ("138".equals(pre)) {
            return 2;
        } else if ("139".equals(pre)) {
            return 3;
        }

        return 4;
    }
}
