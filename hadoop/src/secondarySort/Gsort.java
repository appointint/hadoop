package secondarySort;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;//
import org.apache.hadoop.io.WritableComparable;//
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;
  

public class Gsort {
		
	public static   class StringPart implements WritableComparable<StringPart>{
		private String first;
		private String second;
		public String getFirst(){
			return first;
		}
		public void setFirst(String first){
			this.first=first;
		}
	public String getSecond(){
			return second;
		}
		public void setSecond(String second){
			this.second=second;
		}
		public StringPart(){
			super();
		}
		public StringPart(String first,String second){
			super();
			this.first=first;
			this.second=second;
		}
		@Override
		public void write(DataOutput out) throws IOException{
			out.writeUTF(first);
			out.writeUTF(second);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException{
			this.first=in.readUTF();
			this.second=in.readUTF();
		}

		public int compareTo(StringPart o){
			if(!this.first.equals(o.getFirst())){
				return first.compareTo(o.getFirst());
			}else{
				if(!this.second.equals(o.getFirst())){
					return second.compareTo(o.getFirst());
				}else{
					return 0;
			}}
		}
		
		public int hashCode(){
			final int prime=31;
			int result=1;
			result=prime*result+((first==null)?0:first.hashCode());
			result=prime*result+((second==null)?0:second.hashCode());
			return result;
		}
		
		public boolean equals(Object obj){
			
			if(this==obj)
				return true;
			if(obj==null)
				return false;
			if(getClass()!=obj.getClass())
				return false;
			StringPart other=(StringPart)obj;
			if(first==null){
				if(other.first!=null)
					return false;
			}else if(!first.equals(other.first))
				return false;
			if(second==null){
				if(other.second!=null)
					return false;
			}else if(!second.equals(other.second))
				return false;
			
			return true;
		}
		
		
	}
	
      public static class Map   extends Mapper<LongWritable, Text, StringPart, Text>{  

      public void map(LongWritable key, Text value,  Mapper<LongWritable, Text, StringPart, Text>.Context context ) throws IOException, InterruptedException {  
        String line= value.toString();  
        String[] strings=line.split("\t");
       StringPart temp=new StringPart(strings[0],strings[1]);
       context.write(temp, value);
      }  
     }
     
      public static class Reduce    extends Reducer<StringPart,Text,NullWritable,Text> {  
     private static Text part=new Text("--------------------------");
      public void reduce(StringPart key, Iterable<Text> values, 
    		  Reducer<StringPart,Text,NullWritable,Text>.Context context)throws IOException, InterruptedException {  
    	  context.write(NullWritable.get(), part);
       for (Text val : values) {  
    	 context.write(NullWritable.get(),val);
     	}   
   		}  
      }
    
      public static class Partition extends Partitioner<StringPart, Text>{
    	  @Override
    	  public int getPartition(StringPart key,Text value,int numPartitions){
    		
    		  return Math.abs(key.hashCode())%numPartitions;
    	  }
      }
      public static class Grouping extends WritableComparator{
    	  protected Grouping(){
    		  super(StringPart.class,true);
    	  }
    	  
    	  public int compare(WritableComparable a,WritableComparable b){
    		  StringPart a1=(StringPart)a;
    		  StringPart b1=(StringPart)b;
    		  return a1.getFirst().compareTo(b1.getFirst());
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
        Job job = new Job(conf, "Gsort");  
        job.setJarByClass(Gsort.class);  
        job.setGroupingComparatorClass(Grouping.class);
        job.setPartitionerClass(Partition.class);
        job.setMapperClass(Map.class);  
        job.setMapOutputKeyClass(StringPart.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(Reduce.class);  
        job.setOutputKeyClass(NullWritable.class);  
        job.setOutputValueClass(Text.class);  
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
        job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
      }  



}
