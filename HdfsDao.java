
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;


import java.io.*;
import java.net.URI;


public class HdfsDao<main> {
    private static String HDFS = "hdfs://bigdata:9000";
    private static String utf8 = "UTF-8";
    private static String gbk = "";

    /**
     * 构造函数
     * @param conf
     */

    public HdfsDao(Configuration conf){
        this(HDFS,conf);
    }
    public HdfsDao(String hdfs , Configuration conf){
        this.hdfsPath = hdfs;
        this.conf = conf;
    }
    private String hdfsPath;
    private Configuration conf;

    /**
     * 测试入口
     */
    public static void main(String[] args){
        Configuration conf = config();
        HdfsDao hdfs = new HdfsDao(conf);
        hdfs.ls("/");

    }

    /**
     * Configuration
     */
    public static Configuration config() {
        return new Configuration();
    }

    /**
     * 创建目录
     */
    public void mkdir(String folder){
        Path path = new Path(folder);
        FileSystem fileSystem = null;
        try {
           fileSystem = FileSystem.get(URI.create(hdfsPath),conf);
           if(!fileSystem.exists(path)){
               fileSystem.mkdirs(path);
           }
           System.out.println("创建完成");
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
    /**
     * 删除文件或目录
     */
    public void rm(String folder) {
        Path path = new Path(folder);
        FileSystem fileSystem = null;
        try {
            fileSystem= FileSystem.get(URI.create(hdfsPath), conf);
            boolean a =fileSystem.deleteOnExit(path);
            if (a == true) {
                System.out.println("删除成功");
            } else {
                System.out.println("删除失败");
            }
            /*
            fileSystem.delete(new Path("/words),true);
             */
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 重命名文件
     */
    public void rename(String src ,String dst){
        Path path1 = new Path(src);
        Path path2 = new Path(dst);
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(URI.create(hdfsPath), conf);
            boolean a = fileSystem.rename(path1, path2);
            if (a == true) {
                System.out.println("重命名成功");
            } else {
                System.out.println("重命名失败");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 遍历文件
     */
    public void ls(String folder){
        Path path = new Path(folder);
        FileSystem fileSystem =null;
        try {
            fileSystem = FileSystem.get(URI.create(hdfsPath), conf);
            FileStatus[] list = fileSystem.listStatus(path);
            for (int i=0;i<list.length;i++) {
                System.out.println(list[i].getPath());
                System.out.println(list[i].isDirectory());
                System.out.println(list[i].getLen());
                System.out.println("-------------------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }
    /**
     * 创建文件
     */
    public void createFile(String file,String content){
        FileSystem fileSystem =null;
        byte[] buff = content.getBytes();
        FSDataOutputStream outputStream = null;
        try {
            fileSystem = FileSystem.get(URI.create(hdfsPath), conf);
            outputStream = fileSystem.create(new Path(file));
            outputStream.write(buff,0,buff.length);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 上传文件
     */
    public void copyFile(String local, String remote) {
        FileSystem fileSystem = null;
        FSDataOutputStream outputStream = null;
        FileInputStream fileInputStream = null;
        try {
            fileSystem=FileSystem.get(URI.create(hdfsPath), conf);
            outputStream = fileSystem.create(new Path(remote));
            fileInputStream = new FileInputStream(local);
            IOUtils.copyBytes(fileInputStream,outputStream,2048,true);
//            fileSystem.copyFromLocalFile(new Path(local),new Path(remote));
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 下载文件
     */
    public void download(String remote, String local) {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(URI.create(hdfsPath), conf);
            fileSystem.copyToLocalFile(new Path(remote),new Path(local));
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 查看文件中的内容
     */
    public String cat(String remoteFile) {
        Path path = new Path(remoteFile);
        FileSystem fileSystem = null;
        FSDataInputStream fsDataInputStream = null;
        OutputStream baos = new ByteArrayOutputStream();
        String str = null;
        String trans = null;
        try{
            fileSystem = FileSystem.get(URI.create(hdfsPath), conf);
            fsDataInputStream = fileSystem.open(path);
            IOUtils.copyBytes(fsDataInputStream,baos,2048,false);
            str = ((ByteArrayOutputStream) baos).toString();
            //转码
            //trans = new String(str.getBytes(), 0, str.getBytes().length, utf8);
        }catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(fsDataInputStream);
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return str;
    }

    /**
     * 转换编码
     */
    public static void convert(String oldFile, String oldCharset, String newFile, String newCharset) {
        BufferedReader bin = null;
        FileOutputStream fileOutputStream = null;
        StringBuffer content = new StringBuffer();
        String line = null;
        try{
            bin = new BufferedReader(new InputStreamReader(new FileInputStream(oldFile), oldCharset));
            while ((line = bin.readLine()) != null) {
                content.append(line);
                content.append(System.getProperties());

            }
            bin.close();
            File dir = new File(newFile.substring(0, newFile.length()));
            if(!dir.exists()){
                dir.mkdirs();
            }
            fileOutputStream = new FileOutputStream(newFile);
            Writer out = new OutputStreamWriter(fileOutputStream, newCharset);
            out.write(content.toString());
            out.close();
            fileOutputStream.close();
        }catch(IOException e) {
            e.printStackTrace();
        }
    }
}
