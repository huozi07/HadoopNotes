package org.apache.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class hdfs {
	FileSystem fs = null;
	@Before
	public void init() throws Exception{
		//初始化
		fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), new Configuration(), "root");
	}
	
	@Test
	//上传文件  测试成功
	public void testUpload() throws Exception{
		InputStream in = new FileInputStream("d://words");
		OutputStream out = fs.create(new Path("/inputfile/words"));
		IOUtils.copyBytes(in, out, 1024, true);//in输入源, out输出源头, 1024缓冲区大小 ,true 是否关闭数据流。如果是false，就在finally里关掉
	}
	
	@Test
	//创建文件夹  测试成功
	public void testMkdir() throws IllegalArgumentException, IOException{
		boolean  flag = fs.mkdirs(new Path("/a//test4"));
		System.out.println(flag);//如果创建目录成功会返回true
	}
	
	
	@Test
	//下载文件  测试成功
	public void testDownload() throws IllegalArgumentException, IOException{
		//fs.copyToLocalFile(new Path("/jdk"),new Path("d://jkd111"));
		InputStream in = fs.open(new Path("/a/testtext.txt"));//InputStream  hdfs上的文件
		OutputStream out = new FileOutputStream("d://textdownload");//下载到本地路径 以及重命名后的名字
		IOUtils.copyBytes(in, out, 4096, true);
	}
	
	@Test
	//删除文件   测试成功
	public void testDelFile() throws IllegalArgumentException, IOException{
		boolean flag = fs.delete(new Path("/inpufile"),true);//如果是删除路径  把参数换成路径即可"/a/test4"
		//第二个参数true表示递归删除，如果该文件夹下还有文件夹或者内容 ，会变递归删除，若为false则路径下有文件则会删除不成功
		//boolean flag2 = fs.delete(new Path("/log12345.log"),true);
		//boolean flag3 = fs.delete(new Path("/jdk"),true);
		System.out.println("删除文件 or 路径");
		System.out.println("delete?"+flag);//删除成功打印true
	}
	
	@Test
	//重命名文件   测试成功
	public void testRename() throws IllegalArgumentException, IOException{
		boolean flag = fs.rename(new Path("/input"),new Path("/inputfile"));//第一个参数改名为第二个参数
		String result=flag?"成功":"失败";
		System.out.println("result of rename?"+result);//删除成功打印true
	}
	
	@Test
	//查看文件是否存在     测试成功
	public void CheckFile() throws IllegalArgumentException, IOException{
		boolean  flag = fs.exists(new Path("/a/test3"));
		System.out.println("Exist?"+flag);//如果创建目录成功会返回true
	}
	
	@Test
	//寻找文件在集中位置  测试成功
	public void FileLoc() throws IllegalArgumentException, IOException{
	    FileStatus  filestatus = fs.getFileStatus(new Path("/a/javawrite"));
		//System.out.println("filestatus?"+filestatus);//如果创建目录成功会返回true
		BlockLocation[] blkLocations=fs.getFileBlockLocations(filestatus, 0, filestatus.getLen());//文件开始与结束
		
		int blockLen=blkLocations.length;//块的个数
		System.out.println("---------分割线--------");
		for(int i=0;i<blockLen;i++){
			String[] hosts=blkLocations[i].getHosts();
			System.out.println("block_"+i+"location:"+hosts[0]);
		}
		System.out.println("---------分割线---------");
	
	}
	
	//直接在hdfs上创建文件并在其中输入文字   测试成功
	public void testCreateTextFile() throws IllegalArgumentException, IOException{
		
		byte[] buff="hello hadoop world!\n".getBytes();//想要输入内容
		Path path=new Path("/a/test4/javawrite.txt");//文件存放路径及文件名称
		FSDataOutputStream outputStream=fs.create(path);
		outputStream.write(buff, 0, buff.length);
		System.out.println("输出文件成功");
	
	}
	
	public static void main(String[] args) throws Exception {
		
		hdfs test=new hdfs();
		test.init();
		//选择想要执行的操作
		//test.testRename();
		//test.CheckFile();
		//test.testCreateTextFile();
		// test.testDelFile();
		//test.testDownload();
		//test.testUpload();
		//test.testMkdir();
		//test.FileLoc();
	}
}
