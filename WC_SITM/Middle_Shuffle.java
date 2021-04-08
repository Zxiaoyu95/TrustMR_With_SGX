import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.*;

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
			StringTokenizer stringTokenizer = new StringTokenizer( valueStr);
			Text word = new Text();
			while (stringTokenizer.hasMoreTokens()) {
				String wordValue = stringTokenizer.nextToken();				
                                word.set(wordValue);		
				context.write(word,  new Text("_1"));
			}
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
                                String keyStr=split[0].trim();
				String valueStr=split[1].trim();
				int r = (keyStr.hashCode()&Integer.MAX_VALUE)%numReduceTasks;
				if(r == 0)num0++;if(r == 1)num1++;if(r == 2)num2++;if(r == 3)num3++;if(r == 4)num4++;if(r == 5)num5++;if(r == 6)num6++;if(r == 7)num7++;
				String R = String.valueOf(r);
                                String newV = keyStr + valueStr;
				context.write(new Text(R),new Text(newV));
				}

			@Override
			protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
                                MAX=Math.max(num0,Math.max(num1,Math.max(num2,Math.max(num3,Math.max(num4,Math.max(num5,Math.max(num6,num7)))))));
                               
				if(num0 < MAX){
					for(int i=0; i < (MAX-num0);++i){
						context.write(new Text("0"),new Text("321_1"));
					}
				}
				if(num1 < MAX){
					for(int i=0; i < (MAX-num1);++i){
						context.write(new Text("1"),new Text("321_1"));
					}
				}
				if(num2 < MAX){
					for(int i=0; i < (MAX-num2);++i){
						context.write(new Text("2"),new Text("321_1"));
					}
				}
				if(num3 < MAX){
					for(int i=0; i < (MAX-num3);++i){
						context.write(new Text("3"),new Text("321_1"));
					}
				}
				if(num4 < MAX){
					for(int i=0; i < (MAX-num4);++i){
						context.write(new Text("4"),new Text("321_1"));
					}
				}
				if(num5 < MAX){
					for(int i=0; i < (MAX-num5);++i){
						context.write(new Text("5"),new Text("321_1"));
					}
				}
				if(num6 < MAX){
					for(int i=0; i < (MAX-num6);++i){
						context.write(new Text("6"),new Text("321_1"));
					}
				}
				if(num7 < MAX){
					for(int i=0; i < (MAX-num7);++i){
						context.write(new Text("7"),new Text("321_1"));
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
					String valueStr=i.toString().trim();
					ifdummy = valueStr.substring(0,valueStr.indexOf("_"));
					String count = valueStr.substring(valueStr.indexOf("_")+1,valueStr.length());
					if(!map.containsKey(ifdummy) && ifdummy != "321"){
						map.put(ifdummy, 1);
					}
					else if(map.containsKey(ifdummy) && ifdummy != "321"){
						
						map.put(ifdummy,(int)(map.get(ifdummy)+Integer.valueOf(count)));
					
					}
					
				}
		
				Iterator<String> iterator = map.keySet().iterator();
				
				while(iterator.hasNext()){
				
						String Ikey = iterator.next();
						if(Ikey != "321"){
						context.write(new Text(Ikey.trim()), new Text(String.valueOf(map.get(Ikey))));
						
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
				String keyStr=key.toString().trim();
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

	
