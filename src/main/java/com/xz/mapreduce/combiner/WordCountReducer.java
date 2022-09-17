package com.xz.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author xz
 * KEYIN    Reduce 阶段输入的 key 类型: Text(来自 Map 的输出)
 * VALUEIN  Reduce 阶段输入的 value 类型: IntWritable(来自 Map 的输出)
 * KEYOUT   Reduce 阶段输出的 key 类型: Text
 * VALUEOUT Reduce 阶段输出的 value 类型: IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable outV = new IntWritable();

    // 每次调用 key 执行一次
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        int sum = 0;
        // key:(1,1...)
        for (IntWritable value : values) {
            sum += value.get();
        }
        outV.set(sum);
        //写出
        context.write(key, outV);
    }
}
