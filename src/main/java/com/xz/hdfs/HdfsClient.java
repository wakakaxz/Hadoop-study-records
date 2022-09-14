package com.xz.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @author xz
 * 客户端代码常用套路
 * 1. 获取客户端对象
 * 2. 执行相关的操作命令
 * 3. 关闭资源
 * HDFS Zookeeper
 */
public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接集群 NN 地址
        URI uri = new URI("hdfs://hadoop102:8020");
        // 创建一个配置文件
        Configuration configuration = new Configuration();

        configuration.set("dfs.replication", "2");

        // 用户
        String user = "xz";
        // 1. 获取到了客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        // 3. 关闭资源
        fs.close();
    }

    /**
     * 创建文件夹
     */
    @Test
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {
        fs.mkdirs(new Path("/xiyou/流沙河"));

    }

    /**
     * 上传文件
     * <p>
     * 参数优先级: hdfs-default.xml < hdfs-site.xml < 本地资源目录下的配置文件优先级 < 代码中的 configuration.set() 优先级
     * 新建 hdfs-site.xml 测试优先级
     */
    @Test
    public void testPut() throws IOException {
        // 参数1: 表示删除原数据, 参数2: 是否允许覆盖, 参数3: 原数据路径, 参数4:目的路径
        fs.copyFromLocalFile(false, true, new Path("F:\\sunwukong.txt"), new Path("/xiyou/huaguoshan"));
    }

    /**
     * 下载文件
     */
    @Test
    public void testGet() throws IOException {
        // 参数1:下载后源文件是否删除, 参数2:HDFS原文件路径, 参数3: 目标地址路径windows, 参数4: 开启本地文件校验

        fs.copyToLocalFile(true, new Path("/xiyou/huaguoshan/"), new Path("F:\\"), true);
        // 校验为 false 时, 下载后文件后, 可以看到有一个 crc 校验文件
        // 校验为 true 时, 没有 crc 文件
    }

    /**
     * 删除文件
     */
    @Test
    public void testRm() throws IOException {
        // 参数1: 要删除的路径, 参数2: 是否递归删除

        // 删除文件
//        fs.delete(new Path("/jinguo/wuguo.txt"), false);

        // 删除空目录
//        fs.delete(new Path("/xiyou"), false);

        // 删除非空目录
        fs.delete(new Path("/jinguo"), true);
        // 不递归删除会报异常: /jinguo is non empty'
    }

    /**
     * 文件更名和移动
     */
    @Test
    public void testMv() throws IOException {
        // 参数1: 原文件路径, 参数2: 目标文件路径

        // 文件名称修改
//        fs.rename(new Path("/input/word.txt"), new Path("/input/wcword.txt"));

        // 文件更名和移动
//        fs.rename(new Path("/input/wcword.txt"), new Path("/word.txt"));

        // 文件目录更名
        fs.rename(new Path("/input2"), new Path("/input"));
    }

    /**
     * 获取文件信息
     */
    @Test
    public void fileDetail() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("======" + fileStatus.getPath() + "======");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime()); //修改时间
            System.out.println(fileStatus.getReplication());    // 副本数
            System.out.println(fileStatus.getBlockSize());      // 块大小
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            // 存储块 起始位置, 末尾位置, 存储块的节点 .... 起始位置, 末尾位置, 存储块的节点 ....
            System.out.println(Arrays.toString(blockLocations));

        }
    }

    /**
     * 判断是目录还是文件
     */
    @Test
    public void testFile() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                System.out.println("文件: " + status.getPath().getName());
            } else {
                System.out.println("目录: " + status.getPath().getName());
            }
        }
    }

}
