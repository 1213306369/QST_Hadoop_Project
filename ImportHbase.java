package Round2;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class ImportHbase {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String pattern = "(\\d+.\\d+.\\d+.\\d+) [^ ]* [^ ]* \\[(.*):\\d+:\\d+:\\d+ [^ ]*\\] \"[^ ]+ (/show/\\d+) .*";
			Pattern p = Pattern.compile(pattern);
			Matcher m = p.matcher(line);
			if(m.find()){
				context.write(new Text(m.group(3)), new Text(""));
			}
		}
	}
	public static class Reduce extends TableReducer<Text, Text, NullWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int count = 0;
			for (Text text : values) {
				count++;
			}
			Put put = new Put(Bytes.toBytes(String.valueOf(key)));
			put.add(Bytes.toBytes("show"),Bytes.toBytes("count"),Bytes.toBytes(String.valueOf(count)));
			context.write(NullWritable.get(), put);
		}
	}
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, ClassNotFoundException, InterruptedException {
		String tablename = "show_hxn";
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientport", "2181");
		conf.set("hbase.zookeeper.quorum", "vm10-0-0-2.ksc.com");
		HBaseAdmin admin = new HBaseAdmin(conf);
		 
		 if(admin.tableExists(tablename)){
		        System.out.println("table exists!.......");
		        admin.disableTable(tablename);
		        admin.deleteTable(tablename);
		    }
		 HTableDescriptor htd = new HTableDescriptor(tablename);
		 HColumnDescriptor col = new HColumnDescriptor("show");
		 htd.addFamily(col);
		 admin.createTable(htd);
		 System.out.println("创建表");

		Job job = Job.getInstance(conf,"");
		job.setJarByClass(ImportHbase.class);
		job.setMapOutputKeyClass(Text.class);
		TableMapReduceUtil.initTableReducerJob(tablename, Reduce.class, job);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
