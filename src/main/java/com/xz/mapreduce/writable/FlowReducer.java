package com.xz.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean outV = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        // 1. 遍历集合
        long sumUp = 0;
        long sumDown = 0;
        for (FlowBean value : values) {
            sumUp += value.getUpFlow();
            sumDown += value.getDownFlow();
        }
        // 2. 封装 outV
        outV.setUpFlow(sumUp);
        outV.setDownFlow(sumDown);
        outV.setSumFlow();

        // 3. 写出
        context.write(key, outV);
    }
}
