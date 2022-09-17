package com.xz.mapreduce.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author xz
 * 默认结果会对 key 进行排序
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 获取 job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2. 设置 jar 包路径
        job.setJarByClass(WordCountDriver.class);

        // 3. 关联 mapper 和 reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4. 设置 map 输出的 K V 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 设置最终输出的 K V 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置 Combiner 的类
//        job.setCombinerClass(WordCountCombiner.class);
        // 设置分区数为 0, 最后得到的输出文件只有 part-m-0000 文件, 没有经过汇总的文件
//        job.setNumReduceTasks(0);

        // Combiner 直接使用 Reducer
        job.setCombinerClass(WordCountReducer.class);

        // 6. 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("F:\\input\\inputWCCombiner"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\output\\outputWCCombiner")); // 这个目录不能存在

        // 7. 提交 Job, 参数设置 true, 输出执行过程信息
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 1 : 0);
    }

}
