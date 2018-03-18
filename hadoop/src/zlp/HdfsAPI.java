package zlp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsAPI {
	
	/**
     * 创建目录
     * @param folder
     * @throws IOException
     */
    public void mkdirs(String folder) throws IOException {
    	Configuration conf = new Configuration();
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.40.128:9000"), conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            System.out.println("Create: " + folder);
        }
        fs.close();
    }
    /**
     * 删除文件或目录
     * @param folder
     * @throws IOException
     */
    public void rmr(String folder) throws IOException {
    	Configuration conf = new Configuration();
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.40.128:9000"), conf);
        fs.deleteOnExit(path);
        System.out.println("Delete: " + folder);
        fs.close();
    }
    /**
     * 重命名文件
     * @param src
     * @param dst
     * @throws IOException
     */
    public void rename(String src, String dst) throws IOException {
    	Configuration conf = new Configuration();
        Path name1 = new Path(src);
        Path name2 = new Path(dst);
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.40.128:9000"), conf);
        fs.rename(name1, name2);
        System.out.println("Rename: from " + src + " to " + dst);
        fs.close();
    }
    /**
     * 遍历文件
     * @param folder
     * @throws IOException
     */
    public void ls(String folder) throws IOException {
    	Configuration conf = new Configuration();
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.40.128:9000"), conf);
        FileStatus[] list = fs.listStatus(path);
        System.out.println("ls: " + folder);
        System.out
                .println("==========================================================");
        for (FileStatus f : list) {
            System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(),
                    f.isDir(), f.getLen());
        }
        System.out
                .println("==========================================================");
        fs.close();
    }
    /**
     * 查看文件中的内容
     * @param remoteFile
     * @return
     * @throws IOException
     */
    public String cat(String remoteFile) throws IOException {
    	Configuration conf = new Configuration();
        Path path = new Path(remoteFile);
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.40.128:9000"), conf);
        FSDataInputStream fsdis = null;
        System.out.println("cat: " + remoteFile);

        OutputStream baos = new ByteArrayOutputStream();
        String str = null;
        try {
            fsdis = fs.open(path);
            IOUtils.copyBytes(fsdis, baos, 4096, false);
            str = baos.toString();
        } finally {
            IOUtils.closeStream(fsdis);
            fs.close();
        }
        System.out.println(str);
        return str;
    }
    /**
     * 上传文件到HDFS
     * @param local
     * @param remote
     * @throws IOException
     */
    public void copyFile(String local, String remote) throws IOException {
    	Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.40.128:9000"), conf);
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("copy from: " + local + " to " + remote);
        fs.close();
    }
    /**
     * 从HDFS中下载文件到本地中
     * @param remote
     * @param local
     * @throws IOException
     */
    public void download(String remote, String local) throws IOException {
    	Configuration conf = new Configuration();
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.40.128:9000"), conf);
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from" + remote + " to " + local);
        fs.close();
    }
	public static void main(String[] args) {
		HdfsAPI hd=new HdfsAPI();
		try {
			//上传文件到hdfs
			//hd.copyFile("/usr/local/hadoop/ips.txt", "/user/hadoop/input");
			//下载hdfs文件到本地
			//hd.download("input/table.txt", "/usr/local");
			//hdfs创建文件
			//hd.mkdirs("text");
			//hdfs查看文件
			hd.cat("/input/grade1.txt");
			//hd.rmr("input/ips.txt");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}

