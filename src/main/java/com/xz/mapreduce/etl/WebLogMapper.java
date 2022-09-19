package com.xz.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 1. 获取一行
        String line = value.toString();

        // 2. ETL
        boolean result = parseLog(line, context);

        if (!result) {
            return;
        }
        // 3. 写出
        context.write(value, NullWritable.get());
    }

    private boolean parseLog(String line, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
        // 切割
        String[] split = line.split(" ");
        // 判断字段长度是否大于 11
        return split.length > 11;
    }
}
