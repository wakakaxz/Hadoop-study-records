package com.xz.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream atguiguOut;
    private FSDataOutputStream otherOut;

    public LogRecordWriter(TaskAttemptContext job) throws IOException {
        // 创建两条流
        FileSystem fs = FileSystem.get(job.getConfiguration());

        // Path 最好在 job 中获取, 在 job 中自定义一些配置属性
        atguiguOut = fs.create(new Path("F:\\output\\outputOutputFormat\\atguigu.log"));
        otherOut = fs.create(new Path("F:\\output\\outputOutputFormat\\other.log"));
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String log = text.toString();
        if (log.contains("atguigu")) {
            // writeBytes 默认不换行
            atguiguOut.writeBytes(log + "\n");
            // 中文乱码问题怎么解决?
            // writeChars 因为每个char 长度为 2, 输出结果会产生间隔
//            atguiguOut.writeChars(log);
        } else {
            otherOut.writeBytes(log + "\n");
//            otherOut.writeChars(log);
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 关流
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
