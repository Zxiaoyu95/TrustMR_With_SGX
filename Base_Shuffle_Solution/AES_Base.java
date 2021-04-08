package MRR_Solution;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import MRR_Solution.JAES;

public class AES_Base {
	 static String password="xidian320";
	 static byte[] encryptV=JAES.encrypt("1", password);
	 public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		 @Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String valueStr=value.toString();
			String [] values=valueStr.split("	");
			context.write(new Text(values[2]), new Text(new String(JAES.parseByte2HexStr(encryptV))));
		}
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}
	}
	public static class ShuffleReduce extends Reducer<Text,Text,Text,Text>{
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}
		@Override
		protected void reduce(Text key, Iterable<Text>values,Context context) throws IOException, InterruptedException {
			int sum=0;
			for(Text i:values){
				byte[] value=JAES.decrypt(JAES.parseHexStr2Byte(i.toString()), password);
				String valueStr=new String(value).trim();
				sum+=Integer.valueOf(valueStr);
			}
			byte[] keyB=JAES.decrypt(JAES.parseHexStr2Byte(key.toString()), password);
			String valueStr=new String(keyB).trim();
			context.write(new Text(valueStr.replace("\"", "")), new Text(String.valueOf(sum)));
		
			}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}
		
	}
	
	static class MyPartitioner extends HashPartitioner<Text,Text>{
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			byte[] decryptK=JAES.decrypt(JAES.parseHexStr2Byte(key.toString()), password);
			String keyStr=new String(decryptK);
			return (keyStr.hashCode()&Integer.MAX_VALUE)%numReduceTasks;
			
		}
	}
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
    	long startTime=System.currentTimeMillis();
        Configuration conf = new Configuration();
    	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
         System.err.println("Usage: wordcount <in> [<in>...] <out>");
         System.exit(2);
        }
    	
    	Job job =Job.getInstance(conf,"mapreduce");
    	//Job job =new Job();
    	
    	job.setJarByClass(AES_Base.class);
    	
    	job.setMapperClass(MyMapper.class);
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job, new Path(otherArgs[otherArgs.length - 2]));
    	
    	//job.setCombinerClass(MyCombiner.class);
    	job.setReducerClass(ShuffleReduce.class);
    	job.setPartitionerClass(MyPartitioner.class);
    	job.setOutputKeyClass(Text.class);
    	job.setNumReduceTasks(4);
    	job.setOutputValueClass(Text.class);
    	FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
    
    	int isok = job.waitForCompletion(true)? 0 : 1;
    	
    	long endTime=System.currentTimeMillis();
    	
    	System.exit(isok);
    }
}

	
