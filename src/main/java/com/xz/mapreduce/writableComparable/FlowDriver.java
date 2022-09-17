package com.xz.mapreduce.writableComparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 获取 job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2. 设置 jar
        job.setJarByClass(FlowDriver.class);

        // 3. 关联 mapper, reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 4. 设置 mapper 输出 的 key 和 value 类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 5. 设置最终输出的 key 和 value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 6. 设置数据的输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("F:\\output\\outputFlow"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\output\\outputWritableComparable"));

        // 7. 提交 job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 1 : 0);
    }
}
