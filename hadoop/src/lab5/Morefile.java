package lab5;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;

import hadoopword.WordCount;
import hadoopword.WordCount.IntSumReducer;
import hadoopword.WordCount.TokenizerMapper;

public class Morefile {
	
    public static class Map extends Mapper<LongWritable,Text,NullWritable,Text>{
    	
    	private MultipleOutputs mos;

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			mos=new MultipleOutputs(context);
		}

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			mos.write(NullWritable.get(), value,generateFileName(value)); 
		}
		
		private String generateFileName(Text value){
			String []split=value.toString().split(" ",-1);
			String a=split[0];
			return a+",";
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			mos.close();
		}
    	
    }
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		

	    // 判断output文件夹是否存在，如果存在则删除  
	      Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）  
	     FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件  
	     if (fileSystem.exists(path)) {  
	         fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除  
	     }  

		
		String[]otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length!=2){
			System.err.println("Usage:wordcount<in><out>");
			System.exit(2);
		}
		Job job=new Job(conf,"MulOutput");
		job.setJarByClass(Morefile.class);
		job.setMapperClass(Map.class);
		//job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}