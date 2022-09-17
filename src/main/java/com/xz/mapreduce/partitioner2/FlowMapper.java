package com.xz.mapreduce.partitioner2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xz
 * 读的时候 key / value 默认就是 LongWritable(便文件偏移量) / Text(一行的内容)
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        // 1. 获取一行
        String line = value.toString();
        // 2. 切片
        String[] words = line.split("\t");
        // 3. 拿出想要的数据并封装
        outK.set(words[1]);
        outV.setUpFlow(Long.parseLong(words[words.length - 3]));
        outV.setDownFlow(Long.parseLong(words[words.length - 2]));
        outV.setSumFlow();
        // 4. 写入
        context.write(outK, outV);
    }
}
