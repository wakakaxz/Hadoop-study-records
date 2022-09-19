package com.xz.mapreduce.mapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private HashMap<String, String> pdMap = new HashMap<>();
    private Text outK = new Text();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 获取缓存的文件, 并把文件内容封装到集合 pd.txt
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));

        // 从流中读取数据
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            // 切割
            String[] split = line.split("\t");

            // 赋值
            pdMap.put(split[0], split[1]);
        }
        // 关闭流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 处理 order.txt

        String line = value.toString();
        String[] split = line.split("\t");

        // 获取 pid
        String pname = pdMap.get(split[1]);

        // 获取订单 id 和 订单数量 amount
        outK.set(split[0] + "\t" + pname + "\t" + split[2]);
        context.write(outK, NullWritable.get());
    }
}
