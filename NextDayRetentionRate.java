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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NextDayRetentionRate {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String pattern = "(\\d+.\\d+.\\d+.\\d+) [^ ]* [^ ]* \\[([^ ]*) [^ ]*\\] \"[^ ]+ ([^ ])+.*";
			Pattern p = Pattern.compile(pattern);
			Matcher m = p.matcher(value.toString());
			FileSplit fileSplit =(FileSplit) context.getInputSplit();
			String path = fileSplit.getPath().getParent().getName();
			if(m.find()){
				context.write(new Text(m.group(1)), new Text(path));
			}
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		int sum = 0;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub/
			Configuration conf = context.getConfiguration();
			
			String[] FileName1 = conf.get("FileName1").split("/");
			String filename1=FileName1[FileName1.length-1];
			
			String[] FileName2 = conf.get("FileName2").split("/");
			String filename2=FileName2[FileName2.length-1];
			
			boolean i = false;
			boolean j = false;
			Iterator<Text> vi = values.iterator();
			while(vi.hasNext()){
				String path = vi.next().toString();
				if(path.equals(filename1)){
					i = true;
				}
				if(path.equals(filename2)){
					j = true;
				}
			}
			if(i==true && j==true){
				sum++;
			}
		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new Text(""), new Text(sum + ""));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("FileName1",args[0]);
		conf.set("FileName2",args[1]);
		Job job = Job.getInstance(conf,"");
		job.setJarByClass(NextDayRetentionRate.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		FileInputFormat.setInputPaths(job, new Path(args[0]),new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		boolean status = job.waitForCompletion(true);
				
	}
}
