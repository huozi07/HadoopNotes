package mrdp.ch2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import mrdp.ch2.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MinMaxCountDriver {
    //mapper module
	public static class SOMinMaxCountMapper extends
			Mapper<Object, Text, Text, MinMaxCountTuple> {
		// Our output key and value Writables定义输出的key与value
		
		private Text outUserId = new Text();
		//输出类型为自定义数据类型MinMaxCountTuple类
		private MinMaxCountTuple outTuple = new MinMaxCountTuple();

		// This object will format the creation date string into a Date object
		//将时间数据转为时间类型   指定了时间格式
		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			//解析行数据为map  对应python的字典类型
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

			// Grab the "CreationDate" field since it is what we are finding
			// the min and max value of
			String strDate = parsed.get("CreationDate");//parsed是map  也就是字典类型 由键获得数值

			// Grab the “UserID” since it is what we are grouping by
			String userId = parsed.get("UserId");//获得

			//get will return null if the key is not there
			if (strDate == null || userId == null) {
				// skip this record  第一行数据是空 跳过该数据  map返回值为空
				return;
			}

			try {
				// Parse the string into a Date object
				Date creationDate = frmt.parse(strDate);

				// Set the minimum and maximum date values to the creationDate
				outTuple.setMin(creationDate);//outTuple为自定义数据类型
				outTuple.setMax(creationDate);

				// Set the comment count to 1
				outTuple.setCount(1);

				// Set our user ID as the output key
				outUserId.set(userId);

				// Write out the user ID with min max dates and count
				context.write(outUserId, outTuple);
			} catch (ParseException e) {
				// An error occurred parsing the creation Date string
				// skip this record
			}
		}
	}
    //reducer module
	public static class SOMinMaxCountReducer extends
			Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
		
		private MinMaxCountTuple result = new MinMaxCountTuple();

		@Override
		public void reduce(Text key, Iterable<MinMaxCountTuple> values,
				Context context) throws IOException, InterruptedException {

			// Initialize our result   result也是自定义数据类型  MinMaxCountTuple
			result.setMin(null);
			result.setMax(null);
			int sum = 0;

			// Iterate through all input values for this key
			for (MinMaxCountTuple val : values) {

				// If the value's min is less than the result's min
				// Set the result's min to value's
				if (result.getMin() == null
						|| val.getMin().compareTo(result.getMin()) < 0) {
					result.setMin(val.getMin());
				}

				// If the value's max is less than the result's max
				// Set the result's max to value's
				if (result.getMax() == null
						|| val.getMax().compareTo(result.getMax()) > 0) {
					result.setMax(val.getMax());
				}

				// Add to our sum the count for val
				sum += val.getCount();
			}

			// Set our count to the number of input values
			result.setCount(sum);

			context.write(key, result);
		}
	}
    //main function
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//构建   map
		//String[] otherArgs = new GenericOptionsParser(conf, args)
			//	.getRemainingArgs();
		//input and out put
		//if (otherArgs.length != 2) {
		//	System.err.println("Usage: MinMaxCountDriver <in> <out>");
		//	System.exit(2);
		//}
		
		Job job = new Job(conf, "StackOverflow Comment Date Min Max Count");
		
		//FileInputFormat.setInputPaths(job, new Path("G:/eclipsewokspace/MapReduceDesignPatternData/examplesData/MinMaxCount/inputComments.xml"));
		FileInputFormat.setInputPaths(job, new Path("F:/HDFSinputfile/inputComments.xml"));
		FileOutputFormat.setOutputPath(job, new Path("F:/HDFSoutputfile/MinMaxCountResult"));
		
		job.setJarByClass(MinMaxCountDriver.class);
		job.setMapperClass(SOMinMaxCountMapper.class);
		job.setCombinerClass(SOMinMaxCountReducer.class);
		job.setReducerClass(SOMinMaxCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MinMaxCountTuple.class);
		//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		//FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
    //自定义数据类型
	public static class MinMaxCountTuple implements Writable {
		private Date min = new Date();
		private Date max = new Date();
		private long count = 0;

		private final static SimpleDateFormat frmt = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");

		public Date getMin() {
			return min;
		}

		public void setMin(Date min) {
			this.min = min;
		}

		public Date getMax() {
			return max;
		}

		public void setMax(Date max) {
			this.max = max;
		}

		public long getCount() {
			return count;
		}

		public void setCount(long count) {
			this.count = count;
		}
        //实现接口中方法
		@Override
		public void readFields(DataInput in) throws IOException {
			min = new Date(in.readLong());
			max = new Date(in.readLong());
			count = in.readLong();
		} 
		//实现接口中方法

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(min.getTime());
			out.writeLong(max.getTime());
			out.writeLong(count);
		}

		@Override
		public String toString() {
			return frmt.format(min) + "\t" + frmt.format(max) + "\t" + count;
		}
	}
}
