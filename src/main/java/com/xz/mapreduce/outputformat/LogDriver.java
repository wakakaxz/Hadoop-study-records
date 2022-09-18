package com.xz.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 获取 job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2. 设置 jar 包路径
        job.setJarByClass(LogDriver.class);

        // 3. 关联 mapper 和 reducer
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        // 4. 设置 map 输出的 K V 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5. 设置最终输出的 K V 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 自定义 OutputFormat
        job.setOutputFormatClass(LogOutputFormat.class);


        // 6. 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("F:\\input\\inputOutputFormat"));
        //虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
        //而fileoutputformat要输出一个_SUCCESS文件，所以在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path("F:\\output\\outputOutputFormat")); // 这个目录不能存在

        /**
         * 此时输出的 log 文件没有换行
         * */

        // 7. 提交 Job, 参数设置 true, 输出执行过程信息
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 1 : 0);
    }
}
