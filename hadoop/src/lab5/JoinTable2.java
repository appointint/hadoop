package lab5;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class JoinTable2 {
	public static int time=0;
	public static class Map2 extends Mapper<LongWritable,Text,Text,Text>{

		public void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
//			int i=0;
//			while(line.charAt(i)>='9'||){}
			InputSplit inputSplit = context.getInputSplit();
			String filename = ((FileSplit) inputSplit).getPath().getName().toString();
			System.out.println(filename);
			String []lines=line.split(" ");
			if(filename.equals("join1.txt")){
				String key1=lines[0];
				String value1=lines[1]+" "+lines[2]+" "+lines[3];
				context.write(new Text(key1), new Text(value1));
			}else{
				String key1=lines[0];
				String value1=lines[1];
				context.write(new Text(key1), new Text(value1));
			}
			
			
		}
			
			}
public static class Reduce2 extends Reducer<Text,Text,Text,Text>{

		
		protected void reduce(Text key, Iterable<Text>values,Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
//			if(time==0){
//				
//			}
			String newval="";
			int infornum=0;
			String info[]=new String[10];
			int addnum=0;
			String add[]=new String[10];
//			Iterator ite=values.iterator();
//			while(ite.hasNext()){
//				String record=ite.next().toString();
//				int len=record.length();
//				int i=2;
//				char type=record.charAt(0);
//				if(type=='男'||type=='女'){
//					info[infornum]=record;
//					infornum++;
//				}
//				else{
//					add[addnum]=record;
//					addnum++;
//				}
//			}
			for(Text val:values){
				char type=val.toString().charAt(0);
				if(type=='男'||type=='女'){
					info[infornum]=val.toString();
					infornum++;
				}
				else{
					add[addnum]=val.toString();
					addnum++;
				}
			}
			if(infornum!=0&&addnum!=0){
				for(int n=0;n<infornum;n++){
					for(int m=0;m<addnum;m++){
						newval+=info[n]+" "+add[m];
					}
					
				}
				context.write(key, new Text(newval));
			}
			
		}
}

	public static void main(String[] args)throws Exception {
		// TODO Auto-generated method stub


    Configuration conf = new Configuration();  
// 判断output文件夹是否存在，如果存在则删除  
  Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）  
 FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件  
 if (fileSystem.exists(path)) {  
     fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除  
 }  

	  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
	  Job job = new Job(conf, "join");  
	  job.setJarByClass(JoinTable2.class);  
	  
	  job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
	  job.setMapperClass(Map2.class);   
	  job.setReducerClass(Reduce2.class); 

	    job.setOutputKeyClass(Text.class);  
	    job.setOutputValueClass(Text.class);  

	     FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	     FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	     
	    //FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/root/inputjoin"));  
	    //FileOutputFormat.setOutputPath(job,new Path("hdfs://localhost:9000/user/root/outputjoin"));  
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}