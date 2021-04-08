package shuffle_balance_solution;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
public class Main_Job {
	  
	 static Map<String,Float> PickUp_M = new HashMap<String,Float>();
	 static Map<String,Float> PassenN_M = new HashMap<String,Float>();
	 static Map<Integer,Float> PassenN_Reducer_M = new HashMap<Integer,Float>();
	 static Map<Integer,Float> PickUp_Reducer_M = new HashMap<Integer,Float>();
	 static Map<String,Integer> Key_Reducer_M = new HashMap<String,Integer>(); 
	 static float PassenN_Max_f = (float) 0.71;
	 static float PickUp_Max_f = (float) 0.038;
	 static String password="xidian320";
	 static int numReduceTasks = 2;
	 //static int numReduceTasks = (int) (2/PickUp_Max_f);
	 static String staticStr = "";
	 static int MapSum = 0;
	 static Map<String,Integer> map=new HashMap<String, Integer>();
	 static int CountAll = 0;
	 static float key_f;
	 static byte[] encryptC=JAES.encrypt("1", password);
	 static float dummy_less = PickUp_Max_f*numReduceTasks;
	 static float dummy_more = PassenN_Max_f*numReduceTasks;
	 public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		 @Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			PassenN_M.put("0",(float) 0.00);PassenN_M.put("1",(float) 0.71);PassenN_M.put("2",(float) 0.13);PassenN_M.put("3",(float) 0.04);
			PassenN_M.put("4",(float) 0.02);PassenN_M.put("5",(float) 0.06);PassenN_M.put("6",(float) 0.04);
			PickUp_M.put("2013-1-1",(float) 0.028);PickUp_M.put("2013-1-2",(float) 0.026);PickUp_M.put("2013-1-3",(float) 0.029);PickUp_M.put("2013-1-4",(float) 0.032);PickUp_M.put("2013-1-5",(float) 0.032);PickUp_M.put("2013-1-6",(float) 0.028);
			PickUp_M.put("2013-1-7",(float) 0.027);PickUp_M.put("2013-1-8",(float) 0.029);PickUp_M.put("2013-1-9",(float) 0.030);PickUp_M.put("2013-1-10",(float) 0.032);PickUp_M.put("2013-1-11",(float) 0.035);PickUp_M.put("2013-1-12",(float) 0.034);
			PickUp_M.put("2013-1-13",(float) 0.030);PickUp_M.put("2013-1-14",(float) 0.030);PickUp_M.put("2013-1-15",(float) 0.033);PickUp_M.put("2013-1-16",(float) 0.034);PickUp_M.put("2013-1-17",(float) 0.035);PickUp_M.put("2013-1-18",(float) 0.037);
			PickUp_M.put("2013-1-19",(float) 0.034);PickUp_M.put("2013-1-20",(float) 0.031);PickUp_M.put("2013-1-21",(float) 0.026);PickUp_M.put("2013-1-22",(float) 0.033);PickUp_M.put("2013-1-23",(float) 0.036);PickUp_M.put("2013-1-24",(float) 0.036);
			PickUp_M.put("2013-1-25",(float) 0.036);PickUp_M.put("2013-1-26",(float) 0.038);PickUp_M.put("2013-1-27",(float) 0.032);PickUp_M.put("2013-1-28",(float) 0.030);PickUp_M.put("2013-1-29",(float) 0.032);PickUp_M.put("2013-1-30",(float) 0.033);
			PickUp_M.put("2013-1-31",(float) 0.035);
			for(int i=0;i<numReduceTasks;i++){
				//PassenN_Reducer_M.put(i,PassenN_Max_f);
				PickUp_Reducer_M.put(i,(float) (0.038+0.5));
			}
			Key_Reducer_M.put("key",321);
			super.setup(context);
		}
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String valueStr=value.toString();
			String [] values=valueStr.split("	");
			String keyV = values[0];
			String valuesV = values[1];	
			byte[] decryptK=JAES.decrypt(JAES.parseHexStr2Byte(keyV), password);
			String keyStr=new String(decryptK).trim().replace("\"", "");			
			if(Key_Reducer_M.containsKey(keyStr)){
	              
			}
			else if (!Key_Reducer_M.containsKey(keyStr)){
				//key_f=PassenN_M.get(keyStr);
				key_f=PickUp_M.get(keyStr);
				//for(Map.Entry<Integer, Float> entry : PassenN_Reducer_M.entrySet()){
				for(Map.Entry<Integer, Float> entry : PickUp_Reducer_M.entrySet()){
					if(entry.getValue() >= key_f ){
						//PassenN_Reducer_M.put(entry.getKey(),entry.getValue()-key_f);
						PickUp_Reducer_M.put(entry.getKey(),entry.getValue()-key_f);
						Key_Reducer_M.put(keyStr,entry.getKey());
						break;
					}
					else{
						continue;
					}
				}
				
			}
			context.write(new Text(keyV), new Text(valuesV));
			//context.write(new Text(keyV), new Text(new String(JAES.parseByte2HexStr(encryptC))));
		}
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
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
			super.cleanup(context);
		}
		
	}
	static class MyPartitioner extends HashPartitioner<Text,Text>{
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			byte[] decryptK=JAES.decrypt(JAES.parseHexStr2Byte(key.toString()), password);
			String keyStr=new String(decryptK).trim().replace("\"", "");
			int R = 0;
			
//			if(Key_Reducer_M.containsKey(keyStr)){
				R=Key_Reducer_M.get(keyStr);
//			}
			return R;
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
   	Job job =Job.getInstance(conf,"job");
   	
   	job.setJarByClass(Main_Job.class);
   	
   	job.setMapperClass(MyMapper.class);
   	job.setMapOutputKeyClass(Text.class);
   	job.setMapOutputValueClass(Text.class);
   	FileInputFormat.addInputPath(job, new Path(otherArgs[otherArgs.length - 2]));
   	
   	job.setReducerClass(ShuffleReduce.class);
   	job.setPartitionerClass(MyPartitioner.class);
   	job.setOutputKeyClass(Text.class);
   	job.setNumReduceTasks(numReduceTasks);
   	job.setOutputValueClass(Text.class);
   	FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
   	
   	int isok = job.waitForCompletion(true)? 0 : 1;
   	
   	long endTime=System.currentTimeMillis();
   	
   	System.exit(isok);
   }
}
