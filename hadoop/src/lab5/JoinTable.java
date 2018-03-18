package lab5;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class JoinTable {  
	
	  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{  
		  
		  private Text word1=new Text();
		  private Text word2=new Text();
		  
	    public void map(Object key, Text value, Context context  ) throws IOException, InterruptedException {  
	     
	    		InputSplit inputSplit=(InputSplit)context.getInputSplit();
	    		String filename=((FileSplit)inputSplit).getPath().getName();
	    		System.out.println(filename);
	    		
	    		if(filename.equalsIgnoreCase("join1.txt")){
	    			String line1=value.toString();
	    			word1=new Text(line1);
	    			context.write(new Text("1"), word1);
	    			System.out.println(word1.toString());
	    		}else if(filename.equalsIgnoreCase("join2.txt")){
	    			String line2=value.toString();
	    			word2=new Text(line2);
	    			context.write(new Text("1"), word2);
	    			System.out.println(word2.toString());
	    		}
	    }  
	  }  
	    
	  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {  
		  Text word=new Text();
	    public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {  
	     
	    	String[]arr=new String[100];
	    	int count=0;
	    	System.out.println("reduce");
	    	for (Text val : values) {
				arr[count]=val.toString();
				count++;
			}
	    	System.out.println(arr[0]);
	    	System.out.println(arr[1]);
	    	String s1=arr[0]+" "+arr[1];
	    	System.out.println(s1);
	    	String s2=arr[3]+" "+arr[4];
	    	String[]arr1=s2.split(" ");
	    	String[]arr2=s1.split(" ");
	    	for (int i = 1; i < arr1.length; i=i+3) {
				for (int j = 0; j < arr2.length; j=j+2) {
					if(arr1[i].equals(arr2[j])){
						System.out.println(arr1[i-1]+" "+arr1[i+1]+" "+arr2[j+1]);
						context.write(new Text(arr1[i-1]+" "+arr1[i]+ " "+arr1[i+1]+" "+arr2[j+1]), new Text(" "));
					}else{
						
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
	    job.setJarByClass(JoinTable.class);  
	    job.setMapperClass(TokenizerMapper.class);  
	    job.setReducerClass(IntSumReducer.class);  
	    job.setOutputKeyClass(Text.class);  
	    job.setOutputValueClass(Text.class);  
	    for (int i = 0; i < otherArgs.length - 1; ++i) {  
	      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));  
	    }  
	    FileOutputFormat.setOutputPath(job,  
	      new Path(otherArgs[otherArgs.length - 1]));  
	    System.exit(job.waitForCompletion(true) ? 0 : 1);  
	  }  
	
	
}
/*public class JoinTable {  

	  public static class MapClass extends  Mapper {  
	    	private static Hashtable joinData=new Hashtable();
	    	
	    	public  void configure(JobConf conf){
	    		try {
					Path[] cachefiles=DistributedCache.getLocalCacheFiles(conf);
					if (cachefiles!=null&&cachefiles.length>0) {
						String line;
						String[]tokens;
						BufferedReader joinreader=new BufferedReader(new FileReader(cachefiles[0].toString()));
						try {
							while((line=joinreader.readLine())!=null){
								tokens=line.split("\\t",2);
								joinData.put(tokens[0], tokens[1]);
							}
						} finally {
							joinreader.close();
						}
					}
				} catch (Exception e) {
					System.err.println(""+e);
				}
	    	}

	  	    public void map(Object key, Object value, OutputCollector output,Reporter reporter ) throws IOException, InterruptedException {  
	  	    	Text text=(Text)value;
	  	    	String[] fields=text.toString().split("\\t");
	  	    	String address=(String) joinData.get(fields[2]);
	  	    	if(address!=null){
	  	    		output.collect(new Text(fields[2]),new Text(fields[0]+"\t"+fields[1]+"\t"+address));
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
	    job.setJarByClass(JoinTable.class);  
	    job.setMapperClass(MapClass.class);    
	    DistributedCache.addCacheFile(new Path("/join2.txt").toUri(), conf);
	    job.setOutputKeyClass(Text.class);  
	    job.setOutputValueClass(TextOutputFormat.class);  
	      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
	    System.exit(job.waitForCompletion(true) ? 0 : 1);  
	  }
	
}*/


/*public class JoinTable {  
  
	public static int time=0;
	
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{  
    public void map(Object key, Text value, Context context  ) throws IOException, InterruptedException {  
      String line=value.toString();
      int i=0;
      if(line.contains("tellname")==true||line.contains("addressname")==true)
      //找出数据中的分割点
    	while(line.charAt(i) >= '9'|| line.charAt(i)<='0'){
    		i++;
      }  
    	if(line.charAt(i) >= '9'|| line.charAt(i)<='0'){
    		int j=i-1;
    		while(line.charAt(j)!=' ') j--;
    		String[]values={line.substring(0, j) , line.substring(i)};
    		context.write(new Text(values[1]), new Text("1+"+values[0]));
      } else{
  		int j=i+1;
  		while(line.charAt(j)!=' ') j++;
  		String[]values={line.substring(0, i+1) , line.substring(j)};
  		context.write(new Text(values[0]), new Text("2+"+values[1]));
      }
    }  
  }  
    
  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {  
    public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {  
     
    	int tell=0;
    	String[]telllist=new String[10];
    	int address=0;
    	String[]addresslist=new String[10];
    	Iterator iterator=values.iterator();
    	while(iterator.hasNext()){
    		String record=iterator.next().toString();
    		int len=record.length();
    		int i=2;
    		char type=record.charAt(0);
    		String tellname=new String();
    		String addressname=new String();
    		if(type=='1'){
    			telllist[tell]=record.substring(2);
    			tell++;
    		}else{
    			addresslist[address]=record.substring(2);
    			address++;
    		}
    	}
    	if(tell!=0&&address!=0){
    		for (int i = 0; i < tell; i++) {
				for (int j = 0; j < address; j++) {
					context.write(new Text(telllist[i]),new Text(addresslist[j]));
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
    job.setJarByClass(JoinTable.class);  
    job.setMapperClass(TokenizerMapper.class);  
    job.setCombinerClass(IntSumReducer.class);  
    job.setReducerClass(IntSumReducer.class);  
    job.setOutputKeyClass(Text.class);  
    job.setOutputValueClass(Text.class);  
    for (int i = 0; i < otherArgs.length - 1; ++i) {  
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));  
    }  
    FileOutputFormat.setOutputPath(job,  
      new Path(otherArgs[otherArgs.length - 1]));  
    System.exit(job.waitForCompletion(true) ? 0 : 1);  
  }  
} */ 