package hadoopword;
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;

  

public class Gsort {

	
      public static class Map   extends Mapper<LongWritable, Text, Text, IntWritable>{  
      private Text name=new Text();
      public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {  

    	  StringTokenizer tokenizer=new StringTokenizer(value.toString());
        while(tokenizer.hasMoreTokens()){
        	String strname=tokenizer.nextToken();
        	name=new Text(strname);
        	String strscore=tokenizer.nextToken();
        	int score=Integer.parseInt(strscore);
        	context.write(name, new IntWritable(score));
        }
      }  
     }
     
      public static class Reduce    extends Reducer<Text,IntWritable,Text,IntWritable> {  
  
      public void reduce(Text key, Iterable<IntWritable> values, 
		   Context context)throws IOException, InterruptedException {  
      java.util.List<Integer> scorelist=new ArrayList<Integer>();
       Iterator<IntWritable>iterator=values.iterator();
       while (iterator.hasNext()) {
		scorelist.add(iterator.next().get());
	}
       Collections.sort(scorelist);
       for(Integer score:scorelist){
    	   context.write(key, new IntWritable(score));
       }
   		}  
      }
      
      public static void main(String[] args) throws Exception {  
        Configuration conf = new Configuration();  
        
        // 判断output文件夹是否存在，如果存在则删除  
        Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）  
       FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件  
       if (fileSystem.exists(path)) {  
           fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除  
       }  
        
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
        if (otherArgs.length !=2) {  
          System.err.println("Usage: wordcount <in> [<in>...] <out>");  
          System.exit(2);  
        }  
        Job job = new Job(conf, "Sort");  
        job.setJarByClass(Gsort.class);  
        job.setMapperClass(Map.class);  
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(IntWritable.class);  
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));  
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
      }  
    

}
