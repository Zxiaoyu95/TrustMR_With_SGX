package MRR_Solution;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

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

public class WordCount_MRR1 {
	static int numReduceTasks =7;
/*job1*/
	 public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
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
				context.write(word,  new Text("1"));
			}
	//		context.write(new Text(values[7].replace("\"", "")), new Text("1"));
			
		}
	}
	public static class ShuffleReduce extends Reducer<Text,Text,Text,Text>{
		@Override
		protected void setup(Reducer<Text,Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}
		@Override
		protected void reduce(Text key, Iterable<Text>values,Context context) throws IOException, InterruptedException {
			int count=0;
			for(Text v:values){
				count+=Integer.parseInt(v.toString());
			}
			context.write(key, new Text(String.valueOf(count)));	
		}	
	}
	static class MyCombiner extends Reducer<Text,Text,Text,Text>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int count=0;
			for(Text v:values){
				count+=Integer.parseInt(v.toString());
			}
			context.write(key, new Text(String.valueOf(count)));
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
				int r=(values[0].hashCode()&Integer.MAX_VALUE)%numReduceTasks;
				for(int i=0;i<numReduceTasks;i++){
				context.write(new Text(values[0]),new Text(values[1]+"_"+i+"#"+r));
				}
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
				int count=0;
				Text newKey=new Text();
				for(Text v:values){
					int j=Integer.parseInt(v.toString().substring(v.toString().indexOf("_")+1,v.toString().indexOf("#")));
					int r=Integer.parseInt(v.toString().substring(v.toString().indexOf("#")+1,v.toString().length()));
		            if(j==r){
							count+=Integer.parseInt(v.toString().substring(0,v.toString().indexOf("_")));
							newKey=key;
		            }
				}
				if(count!=0){
				context.write(newKey, new Text(String.valueOf(count)));}
			}
			
		}
		static class MyPartitioner2 extends HashPartitioner<Text,Text>{
			

			@Override
			public int getPartition(Text key, Text value, int numReduceTasks) {
				String vuleStr=value.toString();
				int r = Integer.parseInt(vuleStr.substring(vuleStr.indexOf("_")+1,vuleStr.indexOf("#")));
				return r;
			}
		}
		
    public static void main(String[] args) throws IOException,URISyntaxException, ClassNotFoundException, InterruptedException{
    	long startTime=System.currentTimeMillis();
    	
    	Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
          System.err.println("Usage: wordcount <in> [<in>...] <out>");
          System.exit(2);
        }
	
    	Job job1 =Job.getInstance(conf,"job1"); 
  //  	Job job1 =new Job();
    	
    	job1.setJarByClass(WordCount_MRR1.class);

      
         FileInputFormat.addInputPath(job1, new Path(otherArgs[otherArgs.length - 3]));
      
    	
    	job1.setMapperClass(MyMapper.class);
    	job1.setMapOutputKeyClass(Text.class);
    	job1.setMapOutputValueClass(Text.class);
    	//Partition
    	job1.setCombinerClass(MyCombiner.class);
    	job1.setPartitionerClass(MyPartitioner.class);
    	
    	job1.setReducerClass(ShuffleReduce.class);
    	job1.setOutputKeyClass(Text.class);
    	job1.setNumReduceTasks(7);
    	job1.setOutputValueClass(Text.class);
    	
    	FileOutputFormat.setOutputPath(job1, new Path(otherArgs[otherArgs.length - 2]));
    
        ControlledJob ctrlJob1= new ControlledJob(conf);
        ctrlJob1.setJob(job1);

        Job job2 =Job.getInstance(conf,"job2");
        //Job job2 =new Job();
    	
        job2.setJarByClass(WordCount_MRR1.class);
    	FileInputFormat.setInputPaths(job2, new Path(otherArgs[otherArgs.length - 2]));
    	
    	job2.setMapperClass(MyMapper2.class);
    	job2.setMapOutputKeyClass(Text.class);
    	job2.setMapOutputValueClass(Text.class);
    	//Partition
    	job2.setPartitionerClass(MyPartitioner2.class);
    	
    	job2.setReducerClass(ShuffleReduce2.class);
    	job2.setOutputKeyClass(Text.class);
    	job2.setNumReduceTasks(7);
    	job2.setOutputValueClass(Text.class);
    	
    	FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1]));
    	
        ControlledJob ctrlJob2= new ControlledJob(conf);
        ctrlJob2.setJob(job2);

        if (job1.waitForCompletion(true)) {
                   System.exit(job2.waitForCompletion(true) ? 0 : 1);
        	       }
    	long endTime=System.currentTimeMillis();
    	
    }
}

	
