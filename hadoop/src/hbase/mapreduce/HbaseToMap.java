package hbase.mapreduce;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HbaseToMap {

	public static class WordCountHbaseReaderMapper extends TableMapper<Text, Text> {
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer("");
			for (Entry<byte[], byte[]> // entry是Map中的一个实体（一个key-value对）
			entry : value.getFamilyMap("tags".getBytes()).entrySet()) {
				String str = new String(entry.getValue());
				// 将字节数组转换为String类型
				if (str != null) {
					sb.append(new String(entry.getKey()));
					sb.append(":");
					sb.append(str);
				}
				context.write(new Text(key.get()), new Text(new String(sb)));
			}
		}
	}
	
	public static class WordCountHbaseReaderReduce extends Reducer<Text,Text,Text,Text>{
	    private Text result = new Text();
	    @Override
	    protected void reduce(Text key, Iterable<Text> values,Context context)
	            throws IOException, InterruptedException {
	        for(Text val:values){
	            result.set(val);
	            context.write(key, result);
	        }
	    }
	}
	
	public static void main(String[] args) throws Exception {

		  String tablename = "region_user_test";
		    Configuration conf = HBaseConfiguration.create();
		    //conf.set("hbase.zookeeper.quorum", "Master");
		    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    if (otherArgs.length != 1) {
		      System.err.println("Usage: WordCountHbaseReader <out>");
		      System.exit(2);
		    }
		    Job job = new Job(conf, "WordCountHbaseReader");
		    job.setJarByClass(HbaseToMap.class);
		    //设置任务数据的输出路径；
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		    job.setReducerClass(WordCountHbaseReaderReduce.class);
		    Scan scan = new Scan();
		    TableMapReduceUtil.initTableMapperJob(tablename,scan,WordCountHbaseReaderMapper.class, Text.class, Text.class, job);
		    //调用job.waitForCompletion(true) 执行任务，执行成功后退出；
		    System.exit(job.waitForCompletion(true) ? 0 : 1);


	}
}
