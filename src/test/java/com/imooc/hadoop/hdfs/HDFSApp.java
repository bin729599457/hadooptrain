package com.imooc.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
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

    /**
     * 查看HDFS文件的内容
     * @throws Exception
     */
    @Test
    public void cat()throws Exception{
        FSDataInputStream inputStream=fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(inputStream,System.out,1024);
        inputStream.close();
    }

    /**
     * 重命名
     * @throws Exception
     */
    @Test
    public void rename()throws Exception{
        Path oldPath=new Path("/hdfsapi/test/a.txt");
        Path newPath=new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath,newPath);
    }

    /**
     * 上传文件到HDFS
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile()throws Exception{
        Path localPath=new Path("/Users/binbin/data/hello.txt");
        Path hdfsPath=new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);
    }

    /**
     * 上传文件到HDFS带有进度条显示（上传大文件时使用）
     * @throws Exception
     */
    @Test
    public void copyFromLocalFileWithProgress()throws Exception{

        InputStream inputStream=new BufferedInputStream(
                new FileInputStream("/Users/binbin/data/HeadFirst.pdf"));
        FSDataOutputStream outputStream =fileSystem.create(new Path("/hdfsapi/test/HeadFirst"), new Progressable() {
            public void progress() {
                System.out.print(".");//进度提醒信息
            }
        });

        IOUtils.copyBytes(inputStream,outputStream,4096);
    }

    /**
     * 从HDFS下载文件到本地
     * @throws Exception
     */
    @Test
    public void copyToLocalFile()throws Exception{
        Path localPath=new Path("/Users/binbin/data/");
        Path hdfsPath=new Path("/hdfsapi/test/b.txt");
        fileSystem.copyToLocalFile(hdfsPath,localPath);//注意文件的顺序
    }

    /**
     * 查看某个目录下的所有文件
     * @throws Exception
     */
    @Test
    public void listFiles()throws Exception{
        FileStatus[] fileStatuses=fileSystem.listStatus(new Path("/hdfsapi/test/"));
        for(FileStatus fileStatus:fileStatuses){
            String isDir=fileStatus.isDirectory()?"Directory":"file";
            short replication=fileStatus.getReplication();
            long len=fileStatus.getLen();
            String path=fileStatus.getPath().toString();

            System.out.println(isDir+"\t"+replication+"\t"+len+"\t"+path);
        }
    }

    /**
     *  删除文件/目录
     * @throws Exception
     */
    @Test
    public void delete()throws Exception{
        fileSystem.delete(new Path("/hdfsapi/test/"),true);
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
