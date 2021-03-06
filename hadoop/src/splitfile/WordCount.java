package splitfile;
import java.io.IOException;  
import java.util.StringTokenizer;  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;  
public class WordCount {  
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {  
        private final static IntWritable one = new IntWritable(1);  
        private Text word = new Text();  
        public void map(Object key, Text value, Context context) throws IOException,  
                InterruptedException {  
            StringTokenizer itr = new StringTokenizer(value.toString());  
            while (itr.hasMoreTokens()) {  
                word.set(itr.nextToken());  
                context.write(word, one);  
            }  
        }  
    }  
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {  
        private IntWritable result = new IntWritable();  
        public void reduce(Text key, Iterable<IntWritable> values, Context context)  
                throws IOException, InterruptedException {  
            int sum = 0;  
            for (IntWritable val : values) {  
                sum += val.get();  
            }  
            result.set(sum);  
            context.write(key, result);  
        }  
    }  
    public static class AlphabetOutputFormat extends MultipleOutputFormat<Text, IntWritable> {  
        @Override  
        protected String generateFileNameForKeyValue(Text key, IntWritable value, Configuration conf) {  
            char c = key.toString().toLowerCase().charAt(0);  
            if (c >= 'a' && c <= 'z') {  
                return c + ".txt";  
            }  
            return "other.txt";  
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
        if (otherArgs.length != 2) {  
            System.err.println("Usage: wordcount <in> <out>");  
            System.exit(2);  
        }  
        Job job = new Job(conf, "word count");  
        job.setJarByClass(WordCount.class);  
        job.setMapperClass(TokenizerMapper.class);  
        job.setCombinerClass(IntSumReducer.class);  
        job.setReducerClass(IntSumReducer.class);  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(IntWritable.class);  
        job.setOutputFormatClass(AlphabetOutputFormat.class);//设置输出格式  
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
    }  
} 
