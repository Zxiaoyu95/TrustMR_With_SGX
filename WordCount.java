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

public class WordCount{
	static int numReduceTasks =1;
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
			
		}
	}
	public static class ShuffleReduce extends Reducer<Text,Text,Text,Text>{
                static int keynum = 0;
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
                        keynum++;
			context.write(key, new Text(String.valueOf(count)));	
		}
                @Override
			protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				
			        context.write(new Text("keynum"), new Text(String.valueOf(keynum)));
				super.cleanup(context);
				
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
    	
    	job1.setJarByClass(WordCount.class);

      
         FileInputFormat.addInputPath(job1, new Path(otherArgs[otherArgs.length - 2]));
      
    	
    	job1.setMapperClass(MyMapper.class);
    	job1.setMapOutputKeyClass(Text.class);
    	job1.setMapOutputValueClass(Text.class);
    	//Partition
    	job1.setCombinerClass(MyCombiner.class);
    	job1.setPartitionerClass(MyPartitioner.class);
    	
    	job1.setReducerClass(ShuffleReduce.class);
    	job1.setOutputKeyClass(Text.class);
    	job1.setNumReduceTasks(1);
    	job1.setOutputValueClass(Text.class);
    	
    	FileOutputFormat.setOutputPath(job1, new Path(otherArgs[otherArgs.length - 1]));
    
 
        int isok = job1.waitForCompletion(true)? 0 : 1;
    	long endTime=System.currentTimeMillis();
        System.exit(isok);
    	
    }
}

	
