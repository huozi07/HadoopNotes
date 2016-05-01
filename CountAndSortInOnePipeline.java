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

//实现了一个数据排序demo：从多数据源读取数据，以ip_cookie为key降序输出访问网站次数前Top_k个用户(异常访问用户)
//同时应用程序可以设置低于min_visitvolume的结果不输出。

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
		//选取访问网站次数排名前K位的ip
		int top_k=100;
		//低于min_visitvolume的值不输出
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
			//如果小于k个值就添加进list
			 String boundary = "";
			 if(visitorlist.size()<top_k&&counter>min_visitvolume){
			  boundary=key.toString();//为了处理读入的第k个元素这个边界条件
              visitorlist.add(new VisitorInfo(key.toString(),counter));
              }
		    //Attention
			//此处有一个边界条件处理
			//对于上面这个if,当visitorlist已经有k-1个元素，再来一个元素会满足下面if语句条件,如果前面k-1个元素有小于第K个元素的值，则第k个元素会添加2次进入列表
			
		    //如果达到k个值，后面的值只有大于list中最小的值才会被添加进list   key.toString().equals(boundary)!=true做边界处理用
			//PS:此处也可以把两个if合并为一个if else 但为了逻辑性,所以拆分写
		    if(visitorlist.size()>=top_k&&key.toString().equals(boundary)!=true&&counter>min_visitvolume){
		      Collections.sort(visitorlist, comparator);//应该自定义排序算法的   因为数据已经基本有序  只有一个新加入元素错位
		      VisitorInfo min=(VisitorInfo)(visitorlist.get(visitorlist.size()-1));
			  Long mincount=min.getCount();//最小的值
			  if(counter>mincount){
				  //删除最小的
				  visitorlist.remove(visitorlist.size()-1);
				  //添加大的				  
	              visitorlist.add(new VisitorInfo(key.toString(),counter));
			  }
		      
		    }
        //如果reduce阶段没有key了,将结果排序后输出
		if(!context.nextKey()){
			//在排序一次，这样即使list中值没有达到k个  数据也是倒序输出的
			//如果list中元素达到k个元素，则最后一个加进去的没有排序
			
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

        //设置文件输入路径
		String path1="F:/HDFSinputfile/testmulti1";
		String path2="F:/HDFSinputfile/-ir-activity-photo-clean-20151026-part-00003";
		String outputDir="F:/HDFSoutputfile/IPCountSortSimple22";
		
		//设置多数据源的输入路径  和各自的mapper函数
		job.setMapperClass(IPCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path(path1),new Path(path2));
        
		//路径设置
		Path outputDirIntermediate=new Path(outputDir);

		job.setReducerClass(IPCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, outputDirIntermediate);
		
		//submit
		job.waitForCompletion(true);
		
	}
	
	//arraylist中的自定义对象
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
	//自定义比较器方法
	public static class ComparatorUser implements Comparator{
		 public int compare(Object arg0, Object arg1) {
			 VisitorInfo user0=(VisitorInfo)arg0;
			 VisitorInfo user1=(VisitorInfo)arg1;
			 //首先比较count,count相同再比较ip_cookieid
			  int flag=user0.getCount().compareTo(user1.getCount());
			  if(flag==0){//加了负号，表示降序输出
			   return -user0.getIP_CookieID().compareTo(user1.getIP_CookieID());
			  }else{
			   return -flag;
			  }  
			 }
			}
	

}
