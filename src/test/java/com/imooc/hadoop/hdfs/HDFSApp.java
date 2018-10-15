package com.imooc.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;

import java.net.URI;

/**
 * Hadoop HDFS Java API操作
 */
public class HDFSApp {

    public static final String HDFS_PATH="hdfs://hadoop000:8020";

    FileSystem fileSystem=null;
    Configuration configured=null;

    @Before
    public void setUp()throws Exception{
        configured=new Configuration();
        fileSystem=FileSystem.get(new URI(HDFS_PATH),configured);
    }

    @After
    public void tearDown() throws Exception{
        configured=null;
        fileSystem=null;
        System.out.println("HDFSApp.tearDown");
    }
}
