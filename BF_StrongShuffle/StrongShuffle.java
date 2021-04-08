package BF_StrongShuffle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
public class StrongShuffle {
	static int numReduceTasks =4;
	static String password="xidian320";
	static byte[] encryptV=JAES.encrypt("1", password);
	static ArrayList<String> key_set = new ArrayList<String>();
	
	 public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		    static ArrayList<String> S_key_set = new ArrayList<String>();
			@Override
			protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				
				super.setup(context);
			}
			@Override
			protected void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
				
				String valueStr=value.toString().trim();
				String [] values=valueStr.split("	");
				byte[] decryptK=JAES.decrypt(JAES.parseHexStr2Byte(values[2]), password);
				String strK =new String(decryptK).trim();
				if(!S_key_set.contains(strK)){
					S_key_set.add(strK);
				}
				int r=(strK.hashCode()&Integer.MAX_VALUE)%numReduceTasks;
				byte[] encryptK=JAES.encrypt(strK+"_"+r+"#"+r, password);
				context.write(new Text(new String(JAES.parseByte2HexStr(encryptK))),new Text(new String(JAES.parseByte2HexStr(encryptV))));
			}
			@Override
			protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				//int max = Client.startClient(String.valueOf(S_key_set.size()));

                                int max = Client.startClient(S_key_set);
                                byte[] encryptK=JAES.encrypt("MAX"+"_"+0+"#"+0, password);
                                byte[] encryptV=JAES.encrypt(String.valueOf(max), password);
                                context.write(new Text(new String(JAES.parseByte2HexStr(encryptK))),new Text(new String(JAES.parseByte2HexStr(encryptV))));
				S_key_set.clear();
				super.cleanup(context);
				
			}
		}
		public static class ShuffleReduce extends Reducer<Text,Text,Text,Text>{
			static Map<String,String> result = new HashMap<String,String>();
			@Override
			protected void setup(Reducer<Text,Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
			
				super.setup(context);
			}
			@Override
			protected void reduce(Text key, Iterable<Text>values,Context context) throws IOException, InterruptedException {
				//int count=0;
				byte[] decryptK=JAES.decrypt(JAES.parseHexStr2Byte(key.toString()), password);
				String keylongstr = new String(decryptK).trim().replace("\"", "");
				String mykey = keylongstr.substring(0,keylongstr.indexOf("_"));		
				for(Text value:values){
					int j=Integer.parseInt(keylongstr.substring(keylongstr.indexOf("_")+1,keylongstr.indexOf("#")));
					int r=Integer.parseInt(keylongstr.substring(keylongstr.indexOf("#")+1,keylongstr.length()));
					byte[] decryptV=JAES.decrypt(JAES.parseHexStr2Byte(value.toString()), password);
					String v = new String(decryptV).trim();
					if(!mykey.startsWith("fake")){
						if(j==r && !result.containsKey(mykey)){
							result.put(mykey, "0");
						}
						for(Map.Entry<String,String> str : result.entrySet()){
							 if(j==r && str.getKey().contains(mykey)){
								    int rel=Integer.valueOf(v)+Integer.valueOf(str.getValue());
				            	    result.put(mykey, String.valueOf(rel));
									//count+=Integer.parseInt(v.toString());
				            }
						}
					}
					
				}
//				if(count!=0){
//				context.write(new Text(mykey), new Text(String.valueOf(count)));}
			}	
			@Override
			protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				//int max = Client.startClient("0");
                                ArrayList<String> strArr = null;
				int max = Client.startClient(strArr);
				for(int i =0;i<max-result.size();i++){
					result.put("fake"+i,"$$$");
				}
				for(Map.Entry<String,String> str : result.entrySet()){
					 //if( !str.getValue().equals("0")){
						 context.write(new Text(str.getKey()), new Text(str.getValue()));
		            //}
				}
				result = new HashMap<String,String>();
				super.cleanup(context);
				
			}
		}
		static class MyCombiner extends Reducer<Text,Text,Text,Text>{
            static ArrayList<String> S_key_set = new ArrayList<String>();
            static int max = 0;
            
			@Override
			protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				
				super.setup(context);
				
			}
			@Override
			protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
                                byte[] decryptK=JAES.decrypt(JAES.parseHexStr2Byte(key.toString()), password);
				String keylongstr = new String(decryptK).trim();
                                if(keylongstr.startsWith("MAX")){
                                	for(Text v:values){
                                             byte[] decryptV=JAES.decrypt(JAES.parseHexStr2Byte(v.toString()), password);
                                             max = Integer.parseInt(new String(decryptV).trim());
				         }
                                }
                                else{
                                 int count=0;
				for(Text v:values){
					count+=1;
				}
				byte[] encryptV=JAES.encrypt(String.valueOf(count), password);
				int r=Integer.parseInt(keylongstr.substring(keylongstr.indexOf("#")+1,keylongstr.length()));
				String mykey = keylongstr.substring(0,keylongstr.indexOf("_"));
                if(! S_key_set.contains(mykey)){
					S_key_set.add(mykey);
				}
				for(int i=0;i<numReduceTasks;i++){
					byte[] encryptK=JAES.encrypt(mykey+"_"+i+"#"+r, password);
					context.write(new Text(new String(JAES.parseByte2HexStr(encryptK))),new Text(new String(JAES.parseByte2HexStr(encryptV))));
				     }	
                                }
				
			}
			@Override
			protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				
                 int key_len = S_key_set.size();
                 for(int i =0;i< max-key_len ;i++){
                	 String key = "fake"+i;
                	 int r=(key.hashCode()&Integer.MAX_VALUE)%numReduceTasks;
                	 for(int j=0;j<numReduceTasks;j++){
							byte[] encryptK=JAES.encrypt(key+"_"+j+"#"+r, password);
							byte[] encryptV=JAES.encrypt("0", password);
							context.write(new Text(new String(JAES.parseByte2HexStr(encryptK))), new Text(new String(JAES.parseByte2HexStr(encryptV))));
						}   
                     }
				super.cleanup(context);
				
			}
		}
		static class MyPartitioner extends HashPartitioner<Text,Text>{
			@Override
			public int getPartition(Text key, Text value, int numReduceTasks) {
				byte[] decryptK=JAES.decrypt(JAES.parseHexStr2Byte(key.toString()), password);
				String vuleStr=new String(decryptK).trim();
				int r = Integer.parseInt(vuleStr.substring(vuleStr.indexOf("_")+1,vuleStr.indexOf("#")));
				return r;
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
        		
        		conf.setInt(MRJobConfig.NUM_MAPS,8);
		    	Job job =Job.getInstance(conf,"fullshuffle");
		    	//Job job =new Job();
		    
		    	job.setJarByClass(StrongShuffle.class);
		    	
		    	job.setMapperClass(MyMapper.class);
		    	job.setMapOutputKeyClass(Text.class);
		    	job.setMapOutputValueClass(Text.class);
		    	FileInputFormat.addInputPath(job, new Path(otherArgs[otherArgs.length - 2]));
		    	
		    	job.setCombinerClass(MyCombiner.class);
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

