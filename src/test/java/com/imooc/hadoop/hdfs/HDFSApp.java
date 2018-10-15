package com.imooc.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * Hadoop HDFS Java API操作
 */
public class HDFSApp {

    public static final String HDFS_PATH="hdfs://localhost:9000";

    FileSystem fileSystem=null;
    Configuration configured=null;

    /**
     * 创建文件夹
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void create() throws Exception{
        FSDataOutputStream outputStream =fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        outputStream.write("hello hadoop".getBytes());
        outputStream.flush();
        outputStream.close();
    }

    @Before
    public void setUp()throws Exception{
        configured=new Configuration();
        fileSystem=FileSystem.get(new URI(HDFS_PATH),configured,"binbin");
    }

    @After
    public void tearDown() throws Exception{
        configured=null;
        fileSystem=null;
        System.out.println("HDFSApp.tearDown");
    }
}
