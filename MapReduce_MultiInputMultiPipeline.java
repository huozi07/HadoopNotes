package MultiInputAndChain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;


public class MultiInputIPCount {
	
	public static class IPCountMapper extends Mapper<Object, Text, keyTuple, LongWritable>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			Map<String, String> parsed = ParseLog.transformlineToMap(value.toString());
			
			String IP= parsed.get("IP");
			String CookieID=parsed.get("CookieID");
			
			keyTuple keytuple = new keyTuple(IP,CookieID);

		    context.write(keytuple, new LongWritable(1));

		}
	}
	
	public static class IPCountSortMapper extends Mapper<LongWritable, Text, SortkeyTuple, NullWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			Map<String, String> parsed = ParseLog.IpCountSortMap(value.toString());
			
			String IP= parsed.get("IP");
			String CookieID=parsed.get("CookieID");
			int Count=Integer.parseInt(parsed.get("Count"));
			SortkeyTuple sortkeytuple = new SortkeyTuple(IP,CookieID,Count);
		    context.write(sortkeytuple, NullWritable.get());
		}
	}
	
	
	public static class IPCountSortReducer extends Reducer<SortkeyTuple, NullWritable, SortkeyTuple, NullWritable>{
		
		protected void reduce(SortkeyTuple key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			context.write(key, NullWritable.get());
		}
		
	}
	
	
	public static class IPCountReducer extends Reducer<keyTuple, LongWritable, keyTuple, LongWritable>{

		@Override
		protected void reduce(keyTuple key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
 
			long counter = 0;

			for(LongWritable val : values){ 
				counter +=val.get();
			}
            //��������������ֵ
			//if(counter>=10L)
			context.write(key, new LongWritable(counter));
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.set("mapreduce.client.submit.file.replication", "20");
		Job job = Job.getInstance(conf);
		job.setJarByClass(MultiInputIPCount.class);

        //�����ļ�����·��
		String path1="F:/HDFSinputfile/testmulti1";
		String path2="F:/HDFSinputfile/testmulti2";
		//���ö�����Դ������·��  �͸��Ե�mapper����
		
		job.setMapperClass(IPCountMapper.class);
		job.setMapOutputKeyClass(keyTuple.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path(path1),new Path(path2));
//		ע���к�����������������Դ���ò�ͬmap����ʱʹ��
//		MultipleInputs.addInputPath(job, new Path(path1),
//				TextInputFormat.class, IPCountMapper.class);
//		MultipleInputs.addInputPath(job, new Path(path2),
//				TextInputFormat.class, IPCountMapper.class);
        
		//·������
		Path outputDirIntermediate=new Path("F:/HDFSoutputfile/MultiInputIPCount_intermediate");
		Path MultiInputIPCountSortDir=new Path("F:/HDFSoutputfile/MultiInputIPCountSortResultTest");

		//set reducer's property
		job.setReducerClass(IPCountReducer.class);
		job.setOutputKeyClass(keyTuple.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, outputDirIntermediate);
		
		//submit
		//job.waitForCompletion(true);
		
		int code = job.waitForCompletion(true) ? 0 : 1;
		
		if (code == 0) {
            //�����ҵ�ɹ������и���ҵ��
			Configuration conf2 = new Configuration();
			Job jobsort = Job.getInstance(conf2);
			
			jobsort.setJarByClass(MultiInputIPCount.class);
			
			//set mapper's property
			jobsort.setMapperClass(IPCountSortMapper.class);
			jobsort.setMapOutputKeyClass(SortkeyTuple.class);
			jobsort.setMapOutputValueClass(NullWritable.class);
			FileInputFormat.setInputPaths(jobsort, outputDirIntermediate);
			
			//set reducer's property
			jobsort.setReducerClass(IPCountSortReducer.class);
			jobsort.setOutputKeyClass(SortkeyTuple.class);
			jobsort.setOutputValueClass(NullWritable.class);
			FileOutputFormat.setOutputPath(jobsort, MultiInputIPCountSortDir);
			//submit
			jobsort.waitForCompletion(true);
			
		} 
		//Clean up the intermediate output��ҵִ������ɾ���ļ�
        //FileSystem.get(conf).delete(outputDirIntermediate, true);

	}
	//�Զ�����������1
	public static class keyTuple implements WritableComparable{
		
		private String IP;
		private String CookieID;
		
		public keyTuple(){}
		
		public keyTuple(String IP, String CookieID) {
			super();
			this.IP = IP;
			this.CookieID=CookieID;
		}
		
		@Override
		public String toString() {
			return this.IP + "\t" + this.CookieID;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			this.IP = in.readUTF();
			this.CookieID = in.readUTF();
			
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(IP);
			out.writeUTF(CookieID);
		}
		
		@Override
		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			//return 0 ��ʾ����Ԫ����ͬ����reduceʱkey��ͬ���ۺ�
			//return 1  ����key�����򣬴˴��޷�����value������
			keyTuple k = (keyTuple)o;
			if(this.IP.equals(k.getIP())&&this.CookieID.equals(k.getCookieID())){//�ж��ַ����Ƿ������equals  ���������==
				//System.out.println(0);
				return 0;
			} else {
				return 1;
			}
		}
		
		public void setIP(String iP) {
			this.IP = iP;
		}
		
		public String getIP() {
			return IP;
		}


		public String getCookieID() {
			return CookieID;
		}

		public void setCookieID(String cookieID) {
			this.CookieID = cookieID;
		}

	}
//	@Test
//	//��Ԫ����  ���ο�
//	public void test(){
//		keyTuple k1 = new keyTuple("61.134.102.230", "353e3d477d7ecaeb089532b4ed4b005d");
//		keyTuple k2 = new keyTuple("61.134.102.230", "353e3d477d7ecaeb089532b4ed4b005d");
//		System.out.println(k1.compareTo(k2));
//	}
	//�Զ�����������2
    public static class SortkeyTuple implements WritableComparable{
		
		private String IP;
		private String CookieID;
		private int Count;
		
		public SortkeyTuple(){}
		
		public SortkeyTuple(String IP, String CookieID,int Count) {
			super();//�̳и���Ĺ��캯��
			this.IP = IP;
			this.CookieID=CookieID;
			this.Count=Count;
			
		}
		
		@Override
		public String toString() {
			String strcount=""+this.Count;
			return this.IP + "\t" + this.CookieID+"\t"+strcount;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			this.IP = in.readUTF();
			this.CookieID = in.readUTF();
			this.Count = in.readInt();
			
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(IP);
			out.writeUTF(CookieID);
			out.writeInt(Count);
		}

		@Override
		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			//return 0 ��ʾ����Ԫ����ͬ����reduceʱkey��ͬ���ۺ�
			//return 1  ����key�����򣬴˴��޷�����value������
			SortkeyTuple k = (SortkeyTuple)o;
			
			if(this.Count>=k.Count){//�ж��ַ����Ƿ������equals  ���������==
				//System.out.println(0);
				return -1;//���ǽ�������
			} else {
				return 1;
			}
		}
		
		public void setIP(String iP) {
			this.IP = iP;
		}
		
		public String getIP() {
			return IP;
		}

		public String getCookieID() {
			return CookieID;
		}

		public void setCookieID(String cookieID) {
			this.CookieID = cookieID;
		}

		public int getCount() {
			return Count;
		}

		public void setCount(int count) {
			this.Count = count;
		}
		
	}
	

}
