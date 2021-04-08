package shuffle_in_the_middle_solution;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class Middle_Shuffle {
	static int numReduceTasks =8;
	static String password="xidian320";
	static byte[] encryptC=JAES.encrypt("_1", password);
	 static String dummyStr = "321";
	 static byte[] encryptD=JAES.encrypt(dummyStr, password);
	 
	 
/*job1*/
	 public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String valueStr=value.toString();
			String [] values=valueStr.split("	");
			context.write(new Text(values[2]), new Text(new String(JAES.parseByte2HexStr(encryptC))));
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
		protected void setup(Reducer<Text,Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}
		//@Override
		//protected void reduce(Text key, Iterable<Text>values,Context context) throws IOException, InterruptedException {
		//	for(Text v:values){
		//		context.write(key, v);	
		//	}
		//}	
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
			int s = (int)(Math.random()*numReduceTasks);
			return s;
		}
	}
/*job2*/
	 public static class MyMapper2 extends Mapper<LongWritable,Text,Text,Text>{
                       static int num0 = 0;
	               static int num1 = 0;
	               static int num2 = 0;
	               static int num3 = 0;
	               static int num4 = 0;
	               static int num5 = 0;
	               static int num6 = 0;
	               static int num7 = 0;
                       static int MAX = 0;
			@Override
			protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				super.setup(context);
			}
			@Override
			protected void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
				String[] split=value.toString().split("	");
				byte[] decryptK=JAES.decrypt(JAES.parseHexStr2Byte(split[0]), password);	
				byte[] decryptV=JAES.decrypt(JAES.parseHexStr2Byte(split[1]), password);	
				String keyStr=new String(decryptK).trim();
				String valueStr=new String(decryptV).trim();
				int r = (keyStr.hashCode()&Integer.MAX_VALUE)%numReduceTasks;
				if(r == 0)num0++;if(r == 1)num1++;if(r == 2)num2++;if(r == 3)num3++;if(r == 4)num4++;if(r == 5)num5++;if(r == 6)num6++;if(r == 7)num7++;
				String R = String.valueOf(r);
				byte[] encryptV=JAES.encrypt(split[0], password);
				byte[] encryptR=JAES.encrypt(R, password);
				context.write(new Text(new String(JAES.parseByte2HexStr(encryptR))), new Text(new String(JAES.parseByte2HexStr(encryptV))+new String(split[1])));
				}

			@Override
			protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
                                MAX=Math.max(num0,Math.max(num1,Math.max(num2,Math.max(num3,Math.max(num4,Math.max(num5,Math.max(num6,num7)))))));
				byte[] R1 = JAES.encrypt("0", password);
				byte[] R2 = JAES.encrypt("1", password);
				byte[] R3 = JAES.encrypt("2", password);
				byte[] R4 = JAES.encrypt("3", password);
				byte[] R5 = JAES.encrypt("4", password);
				byte[] R6 = JAES.encrypt("5", password);
				byte[] R7 = JAES.encrypt("6", password);
				byte[] R8 = JAES.encrypt("7", password);
				if(num0 < MAX){
					for(int i=0; i < (MAX-num0);++i){
						context.write(new Text(new String(JAES.parseByte2HexStr(R1))),new Text(new String(JAES.parseByte2HexStr(encryptD))+new String(JAES.parseByte2HexStr(encryptC))));
					}
				}
				if(num1 < MAX){
					for(int i=0; i < (MAX-num1);++i){
						context.write(new Text(new String(JAES.parseByte2HexStr(R2))),new Text(new String(JAES.parseByte2HexStr(encryptD))+new String(JAES.parseByte2HexStr(encryptC))));
					}
				}
				if(num2 < MAX){
					for(int i=0; i < (MAX-num2);++i){
						context.write(new Text(new String(JAES.parseByte2HexStr(R3))),new Text(new String(JAES.parseByte2HexStr(encryptD))+new String(JAES.parseByte2HexStr(encryptC))));
					}
				}
				if(num3 < MAX){
					for(int i=0; i < (MAX-num3);++i){
						context.write(new Text(new String(JAES.parseByte2HexStr(R4))),new Text(new String(JAES.parseByte2HexStr(encryptD))+new String(JAES.parseByte2HexStr(encryptC))));
					}
				}
				if(num4 < MAX){
					for(int i=0; i < (MAX-num4);++i){
						context.write(new Text(new String(JAES.parseByte2HexStr(R5))),new Text(new String(JAES.parseByte2HexStr(encryptD))+new String(JAES.parseByte2HexStr(encryptC))));
					}
				}
				if(num5 < MAX){
					for(int i=0; i < (MAX-num5);++i){
						context.write(new Text(new String(JAES.parseByte2HexStr(R6))),new Text(new String(JAES.parseByte2HexStr(encryptD))+new String(JAES.parseByte2HexStr(encryptC))));
					}
				}
				if(num6 < MAX){
					for(int i=0; i < (MAX-num6);++i){
						context.write(new Text(new String(JAES.parseByte2HexStr(R7))),new Text(new String(JAES.parseByte2HexStr(encryptD))+new String(JAES.parseByte2HexStr(encryptC))));
					}
				}
				if(num7 < MAX){
					for(int i=0; i < (MAX-num7);++i){
						context.write(new Text(new String(JAES.parseByte2HexStr(R8))),new Text(new String(JAES.parseByte2HexStr(encryptD))+new String(JAES.parseByte2HexStr(encryptC))));
					}
				}
			super.cleanup(context);

			}
		}
		public static class ShuffleReduce2 extends Reducer<Text,Text,Text,Text>{
			@Override
			protected void setup(Reducer<Text,Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				super.setup(context);
			}
			@Override
			protected void reduce(Text key, Iterable<Text>values,Context context) throws IOException, InterruptedException {
				int sum=0;
				String ifdummy;
				Map<String,Integer> map = new HashMap<String, Integer>();
				for(Text i:values){
					byte[] value=JAES.decrypt(JAES.parseHexStr2Byte(i.toString()), password);
					String valueStr=new String(value).trim();
					ifdummy = valueStr.substring(0,valueStr.indexOf("_"));
					String count = valueStr.substring(valueStr.indexOf("_")+1,valueStr.length());
					if(!map.containsKey(ifdummy) && ifdummy != "321"){
						map.put(ifdummy, 1);
					}
					else if(map.containsKey(ifdummy) && ifdummy != "321"){
						//Set<String> s = map.keySet();
						//for(String str:s){
							map.put(ifdummy,(int)(map.get(ifdummy)+Integer.valueOf(count)));
						//}
					}
					
//					if(ifdummy != "321"){
//						sum+=Integer.valueOf(count);
//					}
				}
				//if(ifdummy != "321"){
//					byte[] keyB = JAES.decrypt(JAES.parseHexStr2Byte(ifdummy.replace("\"", "").trim()), password);
//					context.write(new Text(new String(keyB)), new Text(String.valueOf(sum)));
					//context.write(new Text(ifdummy.replace("\"", "").trim()), new Text(String.valueOf(sum)));
				//}
				Iterator<String> iterator = map.keySet().iterator();
				
				while(iterator.hasNext()){
					//byte[] keyB = JAES.decrypt(JAES.parseHexStr2Byte(Vstr.replace("\"", "").trim()), password);
					//String strD = new String(keyB).trim();
						String Ikey = iterator.next();
						if(Ikey != "321"){
						context.write(new Text(Ikey.trim()), new Text(String.valueOf(map.get(Ikey))));
						//context.write(new Text(new String (JAES.decrypt(JAES.parseHexStr2Byte(Vstr.trim()), password))), new Text(String.valueOf(map.get(Vstr))));
						} 
					}
				}
			@Override
			protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				super.cleanup(context);

			}
		}
		static class MyPartitioner2 extends HashPartitioner<Text,Text>{
			

			@Override
			public int getPartition(Text key, Text value, int numReduceTasks) {
				byte[] decryptK=JAES.decrypt(JAES.parseHexStr2Byte(key.toString()), password);
				//byte[] decryptV=JAES.decrypt(JAES.parseHexStr2Byte(value.toString()), password);
				String keyStr=new String(decryptK).trim();
				//String valueStr = new String(decryptV);
				int rid =Integer.valueOf(keyStr);
				return rid;		
			}
		}
		
    public static void main(String[] args) throws IOException,URISyntaxException, ClassNotFoundException, InterruptedException{
    
    	Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
         System.err.println("Usage: wordcount <in> [<in>...] <out>");
         System.exit(2);
        }
//job1
    	Job job1 =Job.getInstance(conf,"job1");
    	//Job job1 =new Job();
    	
    	job1.setJarByClass(Middle_Shuffle.class);
    	FileInputFormat.setInputPaths(job1, new Path(otherArgs[otherArgs.length - 3]));
    	
    	job1.setMapperClass(MyMapper.class);
    	job1.setMapOutputKeyClass(Text.class);
    	job1.setMapOutputValueClass(Text.class);
    	//Partition
    	job1.setPartitionerClass(MyPartitioner.class);
    	
    	job1.setReducerClass(ShuffleReduce.class);
    	job1.setOutputKeyClass(Text.class);
    	job1.setNumReduceTasks(1);//reduce
    	job1.setOutputValueClass(Text.class);
    	
    	FileOutputFormat.setOutputPath(job1, new Path(otherArgs[otherArgs.length - 2]));
    	
        ControlledJob ctrlJob1= new ControlledJob(conf);
        ctrlJob1.setJob(job1);

        Job job2 =Job.getInstance(conf,"job2");
        //Job job2 =new Job();
    	
        job2.setJarByClass(Middle_Shuffle.class);
    	FileInputFormat.setInputPaths(job2, new Path(otherArgs[otherArgs.length - 2]));
    
    	job2.setMapperClass(MyMapper2.class);
    	job2.setMapOutputKeyClass(Text.class);
    	job2.setMapOutputValueClass(Text.class);
    	//Partition
    	job2.setPartitionerClass(MyPartitioner2.class);
    	
    	job2.setReducerClass(ShuffleReduce2.class);
    	job2.setOutputKeyClass(Text.class);
    	job2.setNumReduceTasks(numReduceTasks);
    	job2.setOutputValueClass(Text.class);
    	
    	FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1]));
    	
        ControlledJob ctrlJob2= new ControlledJob(conf);
        ctrlJob2.setJob(job2);
        if (job1.waitForCompletion(true)) {
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
 	       }
    }
}

	
