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
import java.io.*;
import java.util.*;

public class WordCount_BF{
	static int numReduceTasks =4;
/*job1*/
	 public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
                static ArrayList<String> S_key_set = new ArrayList<String>();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String valueStr=value.toString();
			StringTokenizer stringTokenizer = new StringTokenizer( valueStr);
			Text word = new Text();
			while (stringTokenizer.hasMoreTokens()) {
				String wordValue = stringTokenizer.nextToken();				
					
                                if(!S_key_set.contains(wordValue)){
					S_key_set.add(wordValue);
				}
                                int r=(wordValue.hashCode()&Integer.MAX_VALUE)%numReduceTasks;
                                String tmp = wordValue+"_"+r+"#"+r;	
                                word.set(tmp);		
				context.write(word,  new Text("1"));
			}
			
		}
                @Override
			protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				//int max = Client.startClient(String.valueOf(S_key_set.size()));

                                int max = Client.startClient(S_key_set);
                                String key = "MAX"+"_"+0+"#"+0;
                                String value = String.valueOf(max);
                              
                                context.write(new Text(key),new Text(value));
				S_key_set.clear();
				super.cleanup(context);
				
			}
	}
	public static class ShuffleReduce extends Reducer<Text,Text,Text,Text>{
                static int keynum = 0;
                static Map<String,String> result = new HashMap<String,String>();
		@Override
		protected void setup(Reducer<Text,Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}
		@Override
		protected void reduce(Text key, Iterable<Text>values,Context context) throws IOException, InterruptedException {
			int count=0;
                        String keylongstr = key.toString().trim().replace("\"", "");
			String mykey = keylongstr.substring(0,keylongstr.indexOf("_"));	

			for(Text v:values){
                                int j=Integer.parseInt(keylongstr.substring(keylongstr.indexOf("_")+1,keylongstr.indexOf("#")));
				int r=Integer.parseInt(keylongstr.substring(keylongstr.indexOf("#")+1,keylongstr.length()));
                                if(!mykey.startsWith("fake")){
						if(j==r && !result.containsKey(mykey)){
							result.put(mykey, "0");
						}
						for(Map.Entry<String,String> str : result.entrySet()){
							 if(j==r && str.getKey().contains(mykey)){
								int rel=Integer.valueOf(v.toString())+Integer.valueOf(str.getValue());
				            	    		result.put(mykey, String.valueOf(rel));
								
				            			}
						}
				}
				
			}
                        keynum++;
				
		}
                @Override
			protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
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
		protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int count=0;
                        String keylongstr = key.toString();
                        if(keylongstr.startsWith("MAX")){
                                for(Text v:values){
                                      max = Integer.parseInt(v.toString());
				}
                        }
                        else{
                              for(Text v:values){
				count+=Integer.parseInt(v.toString());
			      }
                        }

			int r=Integer.parseInt(keylongstr.substring(keylongstr.indexOf("#")+1,keylongstr.length()));
			String mykey = keylongstr.substring(0,keylongstr.indexOf("_"));
                        if(! S_key_set.contains(mykey)){
				S_key_set.add(mykey);
			 }
		        for(int i=0;i<numReduceTasks;i++){
                            String tmpKey = mykey+"_"+i+"#"+r;
			    context.write(new Text(tmpKey),new Text(String.valueOf(count)));
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
                                        String keytmp = key+"_"+j+"#"+r;
                                        String valuetmp = "0";
					context.write(new Text(keytmp), new Text(valuetmp));
				}   
                    	 }
				super.cleanup(context);
		}
	}
	static class MyPartitioner extends HashPartitioner<Text,Text>{
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
                        String vuleStr=key.toString().trim();
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
    	
    	job1.setJarByClass(WordCount_BF.class);

      
         FileInputFormat.addInputPath(job1, new Path(otherArgs[otherArgs.length - 2]));
      
    	
    	job1.setMapperClass(MyMapper.class);
    	job1.setMapOutputKeyClass(Text.class);
    	job1.setMapOutputValueClass(Text.class);
    	//Partition
    	job1.setCombinerClass(MyCombiner.class);
    	job1.setPartitionerClass(MyPartitioner.class);
    	
    	job1.setReducerClass(ShuffleReduce.class);
    	job1.setOutputKeyClass(Text.class);
    	job1.setNumReduceTasks(4);
    	job1.setOutputValueClass(Text.class);
    	
    	FileOutputFormat.setOutputPath(job1, new Path(otherArgs[otherArgs.length - 1]));
    
 
        int isok = job1.waitForCompletion(true)? 0 : 1;
    	long endTime=System.currentTimeMillis();
        System.exit(isok);
    	
    }
}

	
