package Round2;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JumpRate {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String pattern = "(\\d+.\\d+.\\d+.\\d+) ([^ ]*) ([^ ]*) \\[([^ ]* [^ ]*)\\] \"[^ ]+ \\/([^ ]+) [^ ]+\" (\\d+) (\\d+) \"([^\"]*)\" \"([^\"]*)\"";
			
			Pattern p = Pattern.compile(pattern);
			Matcher m = p.matcher(value.toString());
			if(m.find()){
				String ip = m.group(1);
				String type = m.group(5);
				String[] tokens = type.split("/|-|[?]");
				if(tokens.length>0){
					context.write(new Text(tokens[0]), new Text(ip));	
				}				
			}
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int count = 0;
			for (Text text : values) {
				count ++;
			}
			if(count>200){
				context.write(key, new IntWritable(count));
			}	
		}	
	}
		
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"");
		job.setJarByClass(JumpRate.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean status = job.waitForCompletion(true);
	}
}
