package com.xz.mapreduce.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xz
 * KEYIN    Map 阶段输入的 key 的类型: LongWritable
 * VALUEIN  Map 阶段输入的 value 的类型: Text
 * KEYOUT   Map 阶段输出的 key 的类型: Text
 * VALUEOUT Map 阶段输入出 value 的类型: IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    // 每获取一行数据执行一次
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        // 1. map 每次获取一行数据, key 文件的偏移量
        String line = value.toString();

        // 2. 切片
        String[] words = line.split(" ");

        // 3. 循环写出
        for (String word : words) {
            // 封装 outK
            outK.set(word);
            // 写出
            context.write(outK, outV);
            // 所有的 value 按照相同的 key 组成一个可迭代的列表
        }
    }
}
