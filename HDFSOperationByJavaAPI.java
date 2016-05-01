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
		//��ʼ��
		fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), new Configuration(), "root");
	}
	
	@Test
	//�ϴ��ļ�  ���Գɹ�
	public void testUpload() throws Exception{
		InputStream in = new FileInputStream("d://words");
		OutputStream out = fs.create(new Path("/inputfile/words"));
		IOUtils.copyBytes(in, out, 1024, true);//in����Դ, out���Դͷ, 1024��������С ,true �Ƿ�ر��������������false������finally��ص�
	}
	
	@Test
	//�����ļ���  ���Գɹ�
	public void testMkdir() throws IllegalArgumentException, IOException{
		boolean  flag = fs.mkdirs(new Path("/a//test4"));
		System.out.println(flag);//�������Ŀ¼�ɹ��᷵��true
	}
	
	
	@Test
	//�����ļ�  ���Գɹ�
	public void testDownload() throws IllegalArgumentException, IOException{
		//fs.copyToLocalFile(new Path("/jdk"),new Path("d://jkd111"));
		InputStream in = fs.open(new Path("/a/testtext.txt"));//InputStream  hdfs�ϵ��ļ�
		OutputStream out = new FileOutputStream("d://textdownload");//���ص�����·�� �Լ��������������
		IOUtils.copyBytes(in, out, 4096, true);
	}
	
	@Test
	//ɾ���ļ�   ���Գɹ�
	public void testDelFile() throws IllegalArgumentException, IOException{
		boolean flag = fs.delete(new Path("/inpufile"),true);//�����ɾ��·��  �Ѳ�������·������"/a/test4"
		//�ڶ�������true��ʾ�ݹ�ɾ����������ļ����»����ļ��л������� �����ݹ�ɾ������Ϊfalse��·�������ļ����ɾ�����ɹ�
		//boolean flag2 = fs.delete(new Path("/log12345.log"),true);
		//boolean flag3 = fs.delete(new Path("/jdk"),true);
		System.out.println("ɾ���ļ� or ·��");
		System.out.println("delete?"+flag);//ɾ���ɹ���ӡtrue
	}
	
	@Test
	//�������ļ�   ���Գɹ�
	public void testRename() throws IllegalArgumentException, IOException{
		boolean flag = fs.rename(new Path("/input"),new Path("/inputfile"));//��һ����������Ϊ�ڶ�������
		String result=flag?"�ɹ�":"ʧ��";
		System.out.println("result of rename?"+result);//ɾ���ɹ���ӡtrue
	}
	
	@Test
	//�鿴�ļ��Ƿ����     ���Գɹ�
	public void CheckFile() throws IllegalArgumentException, IOException{
		boolean  flag = fs.exists(new Path("/a/test3"));
		System.out.println("Exist?"+flag);//�������Ŀ¼�ɹ��᷵��true
	}
	
	@Test
	//Ѱ���ļ��ڼ���λ��  ���Գɹ�
	public void FileLoc() throws IllegalArgumentException, IOException{
	    FileStatus  filestatus = fs.getFileStatus(new Path("/a/javawrite"));
		//System.out.println("filestatus?"+filestatus);//�������Ŀ¼�ɹ��᷵��true
		BlockLocation[] blkLocations=fs.getFileBlockLocations(filestatus, 0, filestatus.getLen());//�ļ���ʼ�����
		
		int blockLen=blkLocations.length;//��ĸ���
		System.out.println("---------�ָ���--------");
		for(int i=0;i<blockLen;i++){
			String[] hosts=blkLocations[i].getHosts();
			System.out.println("block_"+i+"location:"+hosts[0]);
		}
		System.out.println("---------�ָ���---------");
	
	}
	
	//ֱ����hdfs�ϴ����ļ�����������������   ���Գɹ�
	public void testCreateTextFile() throws IllegalArgumentException, IOException{
		
		byte[] buff="hello hadoop world!\n".getBytes();//��Ҫ��������
		Path path=new Path("/a/test4/javawrite.txt");//�ļ����·�����ļ�����
		FSDataOutputStream outputStream=fs.create(path);
		outputStream.write(buff, 0, buff.length);
		System.out.println("����ļ��ɹ�");
	
	}
	
	public static void main(String[] args) throws Exception {
		
		hdfs test=new hdfs();
		test.init();
		//ѡ����Ҫִ�еĲ���
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
