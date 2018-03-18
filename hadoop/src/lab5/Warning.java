package lab5;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.io.Files;
import com.sun.javafx.collections.MappingChange.Map;

import hadoopword.WordCount;
import hadoopword.WordCount.IntSumReducer;
import hadoopword.WordCount.TokenizerMapper;

public class Warning {


	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
	
		private Text word = new Text();
		int count=0;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			InputSplit inputSplit=(InputSplit)context.getInputSplit();
			String filename=((FileSplit)inputSplit).getPath().getName();
			if(filename.equals("unomal.txt")){
				String line1=value.toString();
				context.write(new Text("comparetxt"), new Text(line1));
			}else{
				String line=value.toString();
				String str="";
				String valuestr="";
				String strAry[]=line.split("[',''']");
				for (int i = 0, j=0; i < strAry.length; i+=4,j+=4) {
					if (strAry[i].equals('w')) {
						Text idname=new Text(strAry[i]);
						str=str+','+strAry[i];
						valuestr=valuestr+','+strAry[j];
						if(!strAry[j].equals(" ")){
							String wurlpro[]=strAry[j].split("['&''=']");
							
							if(wurlpro!=null){
								for (int k = 0,v=1; k < wurlpro.length-1; k+=2,v+=2) {
									context.write(new Text(wurlpro[k]), new Text(wurlpro[v]));
								}
							}
							
						}
					}
				}
			}
			
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		
		java.util.Map<String, Set> map=new HashMap<String,Set>();
		Set<String> set=new HashSet<String>();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			String str1="";
			String value1="";
			String sum="";
			String[]strAry=null;
			String[]kvAry=null;
			int num=0;
			if(!key.toString().equals("comparetxt")){
				for(Text val:values){
					sum+=value1.toString();
					set.add(val.toString());
					num++;
					map.put(key.toString(), set);
				}
			}else{
				for(Text val:values){
					strAry=val.toString().split("[',''']");
					for (int i = 2; i < kvAry.length; i+=3) {
						String wurlpro[]=strAry[i].split("['&''=']");
						
						if(wurlpro!=null){
							for (int k = 0,v=1; k < wurlpro.length-1; k+=2,v+=2) {
								
								if(map.containsKey(wurlpro[k])){
									
									Set set=map.get(wurlpro[k]);
									
									if(str1.contains(wurlpro[v])){
										context.write(new Text("w"), new Text(val));
									}else
										context.write(new Text("n"), new Text(val));
									}
									
								}
								
							}else{
								context.write(new Text("n"), new Text(val));
							}
						}
					
					}
					
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
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(Warning.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
