package lab5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class JoinTable3 {
	public static class Map extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] split = value.toString().split(" ");
			int i = 0;
			for (String s : split) {
				if (s.charAt(0) >= '0' && s.charAt(0) <= '9') {
					if (i != 0){
						//#和%分别用于区分value的值的来源，在reduce中可便于按顺序输出
						value=new Text("#"+value.toString());
						context.write(new Text(s), value);
					}
					else
						context.write(new Text(s), new Text("%"+split[1]));
					break;
				}
				i++;
			}
		}
	}

		public static class Reduce extends Reducer<Text, Text, Text, Text> {

			protected void reduce(Text key, Iterable<Text> values,
					Context context) throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				String result = "";
				//Text value=null;
				for (Text t : values) {
					if(t.toString().charAt(0)=='#')
						result = t.toString().substring(1) + " "+result;
					else
						result += t.toString().substring(1) + " ";
				}
				result = result.trim();
				context.write(new Text(result), new Text());
				
				/*
				 * 由于每个values中都包含两个值，故在输出时根据值前的#和%进行区分，并分别作为新的key-value输出
				 * 此方法对多个文本进行合并输出时会存在问题，会致使值的丢失
				 */
//				for (Text t : values) {
//					if(t.toString().charAt(0)=='#')
//						key =new Text(t.toString().substring(1));
//					else
//						value = new Text(t.toString().substring(1));
//				}
//				context.write(key, value);
			}
		}

		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();

			String[] otherArgs = new GenericOptionsParser(conf, args)
					.getRemainingArgs();
			if (otherArgs.length != 2) {
				System.err.println("Usage: wordcount <in> [<in>...] <out>");
				System.exit(2);
			}
			Job job = new Job(conf, "Merge");
			job.setJarByClass(JoinTable3.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			for (int i = 0; i < otherArgs.length - 1; ++i) {
				FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
			}
			FileOutputFormat.setOutputPath(job, new Path(
					otherArgs[otherArgs.length - 1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		}
}