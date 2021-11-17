package cn.itcast.hdfs_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import org.apache.commons.io.IOUtils;

public class HdfsApiDemo {



    /**
     * hdfs 的权限访问控制
     */
    @Test
    public void 权限访问控制() throws Exception {
        //1: 获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020")
                , new Configuration());

        fileSystem.copyToLocalFile(new Path("/a.txt")
                , new Path("D://a4.txt"));

        fileSystem.close();

    }

    /**
     * hdfs 上传文件
     */
    @Test
    public void upLoadFile() throws Exception {
        //1: 获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020")
                , new Configuration());

        fileSystem.copyFromLocalFile(new Path("D://set.xml")
                , new Path("/hello/mydir/test"));

        fileSystem.close();

    }

    /**
     * hdfs 下载文件
     */
    @Test
    public void downLoadFile2() throws Exception {
        //1: 获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020")
                , new Configuration());

        fileSystem.copyToLocalFile(new Path("/a.txt")
                , new Path("D://hello/mydir/test"));


        fileSystem.close();

    }


    /**
     * hdfs 下载文件
     */
    @Test
    public void downLoadFile() throws Exception {
        //1: 获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020")
                , new Configuration());

        FSDataInputStream inputStream = fileSystem.open(new Path("/timer.txt"));
        FileOutputStream outputStream = new FileOutputStream(new File("e:\\timer.txt"));

        IOUtils.copy(inputStream, outputStream);
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();

    }

    /**
     * hdfs 上创建文件夹
     */
    @Test
    public void mkdirs() throws Exception {
        //1: 获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020")
                , new Configuration());

        boolean mkdirs = fileSystem.mkdirs(new Path("/hello/mydir/test"));

        fileSystem.close();

    }


    /**
     * hdfs文件的遍历
     */
    @Test
    public void listFiles() throws Exception {
        //1: 获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020")
                , new Configuration());

        //2: 调用方法listFiles获取 / 目录所有的文件信息
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);

        //3: 遍历迭代器
        while (iterator.hasNext()){
            LocatedFileStatus next = iterator.next();
            System.out.println(next.getPath().toString());

        }
        fileSystem.close();

    }


    /**
     * 方式4
     * @throws Exception
     */
    @Test
    public void getFileSystem4() throws Exception {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://node01:8020")
                , new Configuration());

        System.out.println(fileSystem);
    }


    /**
     * 方式3
     * @throws Exception
     */
    @Test
    public void getFileSystem3() throws Exception { //1: 创建Configuration对象
        Configuration configuration = new Configuration();

        //2: 设置文件系统的类型
        configuration.set("fs.defaultFS", "hdfs://node01:8020");

        //3: 获取指定的文件系统
        FileSystem fileSystem = FileSystem.newInstance(configuration);

        //4: 输出
        System.out.println(fileSystem);
    }

    /**
     * 方式2
     * @throws Exception
     */
    @Test
    public void getFileSystem2() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020")
                , new Configuration());

        System.out.println(fileSystem);
    }



    /**
     * 方式1
     * @throws Exception
     */
    @Test
    public void getFileSystem1() throws Exception {
        //1: 创建Configuration对象
        Configuration configuration = new Configuration();

        //2: 设置文件系统的类型
        configuration.set("fs.defaultFS", "hdfs://node01:8020");

        //3: 获取指定的文件系统
        FileSystem fileSystem = FileSystem.get(configuration);

        //4: 输出
        System.out.println(fileSystem);
    }


    @Test
    public void urlHdfs()throws Exception{
        //第一步：注册hdfs的URL
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        //获取文件输入流
        InputStream inputStream = new URL("hdfs://node01:8020/a.txt").openStream();

        //获取文件输出流
        FileOutputStream outputStream = new FileOutputStream(new File("D:\\hello.txt"));

        //实现文件的拷贝
        IOUtils.copy(inputStream, outputStream);

        //关闭流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }


}
