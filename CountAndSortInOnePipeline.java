package Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;


import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import MsnAntiSpam.ParseLog;

//ʵ����һ����������demo���Ӷ�����Դ��ȡ���ݣ���ip_cookieΪkey�������������վ����ǰTop_k���û�(�쳣�����û�)
//ͬʱӦ�ó���������õ���min_visitvolume�Ľ���������

public class IPCountSortSimple {
	
	public static class IPCountMapper extends Mapper<Object, Text, Text, LongWritable>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			Map<String, String> parsed = ParseLog.transformlineToMap(value.toString());
			
			String IP= parsed.get("IP");
			String CookieID=parsed.get("CookieID");
	
		    context.write(new Text(IP+"\t"+CookieID), new LongWritable(1));

		}
	}
	
	public static class IPCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		
		List visitorlist=new ArrayList();
		ComparatorUser comparator=new ComparatorUser();
		//ѡȡ������վ��������ǰKλ��ip
		int top_k=100;
		//����min_visitvolume��ֵ�����
		long min_visitvolume=50;

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
 
			long counter = 0;

			for(LongWritable val : values){ 
				counter +=val.get();
			}
			//String readkey=key.toString();
			//long ccount=counter;
			//context.write(key, new LongWritable(counter));
			//���С��k��ֵ����ӽ�list
			 String boundary = "";
			 if(visitorlist.size()<top_k&&counter>min_visitvolume){
			  boundary=key.toString();//Ϊ�˴������ĵ�k��Ԫ������߽�����
              visitorlist.add(new VisitorInfo(key.toString(),counter));
              }
		    //Attention
			//�˴���һ���߽���������
			//�����������if,��visitorlist�Ѿ���k-1��Ԫ�أ�����һ��Ԫ�ػ���������if�������,���ǰ��k-1��Ԫ����С�ڵ�K��Ԫ�ص�ֵ�����k��Ԫ�ػ����2�ν����б�
			
		    //����ﵽk��ֵ�������ֵֻ�д���list����С��ֵ�Żᱻ��ӽ�list   key.toString().equals(boundary)!=true���߽紦����
			//PS:�˴�Ҳ���԰�����if�ϲ�Ϊһ��if else ��Ϊ���߼���,���Բ��д
		    if(visitorlist.size()>=top_k&&key.toString().equals(boundary)!=true&&counter>min_visitvolume){
		      Collections.sort(visitorlist, comparator);//Ӧ���Զ��������㷨��   ��Ϊ�����Ѿ���������  ֻ��һ���¼���Ԫ�ش�λ
		      VisitorInfo min=(VisitorInfo)(visitorlist.get(visitorlist.size()-1));
			  Long mincount=min.getCount();//��С��ֵ
			  if(counter>mincount){
				  //ɾ����С��
				  visitorlist.remove(visitorlist.size()-1);
				  //��Ӵ��				  
	              visitorlist.add(new VisitorInfo(key.toString(),counter));
			  }
		      
		    }
        //���reduce�׶�û��key��,�������������
		if(!context.nextKey()){
			//������һ�Σ�������ʹlist��ֵû�дﵽk��  ����Ҳ�ǵ��������
			//���list��Ԫ�شﵽk��Ԫ�أ������һ���ӽ�ȥ��û������
			
			Collections.sort(visitorlist, comparator);
			
		    for (int i=0;i<visitorlist.size();i++){
		    	VisitorInfo user_temp=(VisitorInfo)visitorlist.get(i);
		        //System.out.println(user_temp.getIP_CookieID()+","+user_temp.getCount());
		    	 String testkey2=user_temp.getIP_CookieID();
		        context.write(new Text(user_temp.getIP_CookieID()), new LongWritable(user_temp.getCount()));
		        }
        }
		}

		}
		
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(IPCountSortSimple.class);

        //�����ļ�����·��
		String path1="F:/HDFSinputfile/testmulti1";
		String path2="F:/HDFSinputfile/-ir-activity-photo-clean-20151026-part-00003";
		String outputDir="F:/HDFSoutputfile/IPCountSortSimple22";
		
		//���ö�����Դ������·��  �͸��Ե�mapper����
		job.setMapperClass(IPCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path(path1),new Path(path2));
        
		//·������
		Path outputDirIntermediate=new Path(outputDir);

		job.setReducerClass(IPCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, outputDirIntermediate);
		
		//submit
		job.waitForCompletion(true);
		
	}
	
	//arraylist�е��Զ������
	public static class VisitorInfo {
		 String IP_CookieID;
		 Long Count;
		 
		 public VisitorInfo(String key,Long Count){
		  this.IP_CookieID=key;
		  this.Count=Count;
		 }
		 
		public VisitorInfo() {
			// TODO Auto-generated constructor stub
		}

		public String getIP_CookieID() {
			return this.IP_CookieID;
		}

		public void setIP_CookieID(String iP_CookieID) {
			this.IP_CookieID = iP_CookieID;
		}

		public Long getCount() {
			return this.Count;
		}

		public void setCount(Long count) {
			this.Count = count;
		}
		
		}
	//�Զ���Ƚ�������
	public static class ComparatorUser implements Comparator{
		 public int compare(Object arg0, Object arg1) {
			 VisitorInfo user0=(VisitorInfo)arg0;
			 VisitorInfo user1=(VisitorInfo)arg1;
			 //���ȱȽ�count,count��ͬ�ٱȽ�ip_cookieid
			  int flag=user0.getCount().compareTo(user1.getCount());
			  if(flag==0){//���˸��ţ���ʾ�������
			   return -user0.getIP_CookieID().compareTo(user1.getIP_CookieID());
			  }else{
			   return -flag;
			  }  
			 }
			}
	

}
