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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
public class Offline_Phase{
	 static String password="xidian320";
	 static byte[] encryptC=JAES.encrypt("1", password);
	 static int numReduceTasks =10;
	 static String staticStr = "";
	 static int MapSum = 0;
	 static Map<String,Integer> map=new HashMap<String, Integer>();
	 static int CountAll = 0;
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
			String valuesV = values[2];			
			context.write(new Text(valuesV), new Text(new String(JAES.parseByte2HexStr(encryptC))));
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
			
			for(Text i:values){
				MapSum++;
				staticStr = String.valueOf(MapSum);			
				context.write(key, new Text(new String(JAES.parseByte2HexStr(encryptC))));
			    }
			MapSum=0;
			byte[] Key=JAES.decrypt(JAES.parseHexStr2Byte(key.toString()), password);
			String KeyStr=new String(Key).trim();
			map.put(KeyStr,Integer.valueOf(staticStr));
			}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : map.entrySet()){
            	CountAll = CountAll + entry.getValue();
            }
            for (Map.Entry<String, Integer> pram : map.entrySet()){
            	float f = ((float)pram.getValue()/CountAll);
            	String fStr = String.valueOf(f);
            	//context.write(new Text(pram.getKey()), new Text(fStr+"_"+String.valueOf(pram.getValue())));
            }
            CountAll = 0;
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
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
    	long startTime=System.currentTimeMillis();
    	
    	Configuration conf = new Configuration();
    	 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
         if (otherArgs.length < 2) {
         System.err.println("Usage: wordcount <in> [<in>...] <out>");
         System.exit(2);
        }
   	Job job =Job.getInstance(conf,"job");
    	
    	job.setJarByClass(Offline_Phase.class);
    	
    	job.setMapperClass(MyMapper.class);
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job, new Path(otherArgs[otherArgs.length - 2]));
    	
    	job.setReducerClass(ShuffleReduce.class);
    	job.setPartitionerClass(MyPartitioner.class);
    	job.setOutputKeyClass(Text.class);
    	job.setNumReduceTasks(10);
    	job.setOutputValueClass(Text.class);
    	FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
    	
    	int isok = job.waitForCompletion(true)? 0 : 1;
    	
    	long endTime=System.currentTimeMillis();
    	
    	System.exit(isok);
    }
}

	
