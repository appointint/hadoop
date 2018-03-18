package hbase.mapreduce;


import java.io.IOException;  
   import java.util.StringTokenizer;  
     
   import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;  
   import org.apache.hadoop.mapreduce.Job;  
   import org.apache.hadoop.mapreduce.Mapper;  
   import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
   import org.apache.hadoop.util.GenericOptionsParser;
import org.datanucleus.transaction.jta.JOnASTransactionManagerLocator;

import breeze.linalg.mapActiveValues;  
     
   public class MapToHbase {
     
     public static class TokenizerMapper   
          extends Mapper<Object, Text, Text, IntWritable>{  
         
       private final static IntWritable one = new IntWritable(1);  
       private Text word = new Text();  
           
       public void map(Object key, Text value, Context context  
                       ) throws IOException, InterruptedException {  
         StringTokenizer itr = new StringTokenizer(value.toString());  
         while (itr.hasMoreTokens()) {  
           word.set(itr.nextToken());  
           context.write(word, one);  
         }  
       }  
     }  
       
     public static class IntSumReducer   
          extends TableReducer<Text,IntWritable,NullWritable> {  
   	  
   	  @SuppressWarnings("deprecation")
	public void reduce(Text key, Iterable<IntWritable> values,   
                 Context context  ) throws IOException, InterruptedException {  
   		 
   		  	int sum = 0;  
   		  	
             for (IntWritable val : values) {  
               sum += val.get();  
             }  
   		  
             Put put=new Put(Bytes.toBytes(key.toString()));
             put.add(Bytes.toBytes("content"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));             
             context.write(NullWritable.get(),put);
             
   	  }
     
     }
     
     public static void main(String[] args) throws Exception {  
   	  

         Configuration conf = new Configuration();  
    
       String tablename="maptohbase";
       conf.set(TableOutputFormat.OUTPUT_TABLE,tablename);
       
       Job job = new Job(conf, "word_count_table");  
       job.setJarByClass(MapToHbase.class); 
       job.setNumReduceTasks(3);
       job.setMapperClass(TokenizerMapper.class);  
       job.setReducerClass(IntSumReducer.class);   
       
       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(IntWritable.class);
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TableOutputFormat.class);
       
      FileInputFormat.addInputPath(job, new Path("hdfs://192.168.19.128:9000/zinput/*"));  
    
       System.exit(job.waitForCompletion(true) ? 0 : 1);  
     }
}

