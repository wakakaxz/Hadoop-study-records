package com.xz.mapreduce.partitioner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        // Text 手机号
        String phone = text.toString();
        String pre = phone.substring(0, 3);

        int partition = 4;
        if ("136".equals(pre)) {
            partition = 0;
        } else if ("137".equals(pre)) {
            partition = 1;
        } else if ("138".equals(pre)) {
            partition = 2;
        } else if ("139".equals(pre)) {
            partition = 3;
        }

        return partition;
    }
}
