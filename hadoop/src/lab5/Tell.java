package lab5;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.util.Tool;  
import org.apache.hadoop.util.ToolRunner;  
import java.io.IOException;  
  
  
public class Tell extends Configured implements Tool {  
  
    enum Counter {  
        LINESKIP; // 出错的行  
    }  
  
    @Override  
   public int run(String[] args) throws Exception {  
        Configuration conf = getConf();  
        
     // 判断output文件夹是否存在，如果存在则删除  
        Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）  
       FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件  
       if (fileSystem.exists(path)) {  
           fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除  
       } 
        
        Job job = new Job(conf, "PhoneTest"); // 任务名  
        job.setJarByClass(Tell.class); // 指定Class  
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class); // 调用Map类作为Mapper任务代码  
        job.setReducerClass(Reduce.class); // 调用Reduce类作为Reducer任务代码  
        job.setOutputFormatClass(TextOutputFormat.class);  
        job.setOutputKeyClass(Text.class); // 指定输出的Key的格式(KEYOUT)  
        job.setOutputValueClass(Text.class); // 指定输出的Value的格式(VALUEOUT)  
        job.waitForCompletion(true);  
          
        return job.isSuccessful() ? 0 : 1;  
    }  
  
    public static class Map extends  
            Mapper<LongWritable, Text, Text, Text> {    //<KEYIN, VALUEIN, KEYOUT, VALUEOUT>  
        @Override  
        protected void map(LongWritable key, Text value, Context context)  
                throws IOException, InterruptedException {  
            try {  
                // key - 行号 value - 一行的文本  
                String line = value.toString();    //13510000000 10086(13510000000拨打10086)  
                // 数据处理  
                String[] lineSplit = line.split(" ");  
                String phone1 = lineSplit[0];  
                String phone2 = lineSplit[1];  
                  
                context.write(new Text(phone2), new Text(phone1));    // 输出 key \t value  
            } catch (java.lang.ArrayIndexOutOfBoundsException e) {  
                context.getCounter(Counter.LINESKIP).increment(1); // 出错令计数器+1  
            }  
        }  
  
    }  
      
    public static class Reduce extends Reducer<Text, Text, Text, Text> {      
          
        @Override  
        protected void reduce(Text key, Iterable<Text> values,  
                Context context)  
                throws IOException, InterruptedException {  
            String valueStr;  
            String out = "";  
            for(Text value:values){  
                valueStr = value.toString() + "|";  
                out += valueStr;  
            }  
            // 输出 key \t value（如果我们的输出结果不是key \t value格式，那么我们的key可定义为NullWritable，而value使用key与value的组合。）  
            context.write(key, new Text(out));      
        }  
    }  
      
    public static void main(String[] args) throws Exception {  
        //运行任务  
        int res = ToolRunner.run(new Configuration(), new Tell(), args);  
        System.exit(res);  
    }  
}  
