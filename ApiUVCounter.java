package Round2;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ApiUVCounter {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String pattern = "(\\d+.\\d+.\\d+.\\d+) ([^ ]*) ([^ ]*) \\[([^ ]* [^ ]*)\\] \"[^ ]* (.*)";
			Pattern p = Pattern.compile(pattern);
			Matcher m = p.matcher(value.toString());
			if(m.find()){
				String line = m.group(5);
				String ip = m.group(1);
				if(line.indexOf("Android")!=-1||line.indexOf("iOS")!=-1){
					context.write(new Text(ip), new Text(line));
				}
			}
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		int AndroidCount = 0;
		int iOSCount = 0;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			boolean android = true;
			boolean ios = true;
			Iterator<Text> vi = values.iterator();
			while(vi.hasNext()){
				String api = vi.next().toString();
				if(api.indexOf("Android")!=-1&& android == true){
					AndroidCount++;
					android = false;
				}else if(api.indexOf("iOS")!=-1&& ios == true){
					iOSCount++;
					ios = false;
				}
			}
		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new Text("AndroidCount"), new Text(AndroidCount+""));
			context.write(new Text("iOSCount"), new Text(iOSCount+""));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"");
		job.setJarByClass(ApiUVCounter.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean status = job.waitForCompletion(true);
	}

}
