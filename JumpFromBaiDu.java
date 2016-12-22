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

public class JumpFromBaiDu {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String pattern = "(\\d+.\\d+.\\d+.\\d+) ([^ ]*) ([^ ]*) \\[([^ ]* [^ ]*)\\] \"([^ ]+ [^ ]+ [^ ]+)\" (\\d+) (\\d+) \"([^\"]*)\" \"[^ ]* [^ ]* [^ ]* \\+[^ ]*\\:\\//([^ ]*\\/[^ ]*)\\)\"";
			Pattern p = Pattern.compile(pattern);
			Matcher m = p.matcher(value.toString());
			if(m.find()){
				String url = m.group(9);
				context.write(new Text(m.group(1)), new Text(url));
			}
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		int count = 0;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Iterator<Text> vi = values.iterator();
			while(vi.hasNext()){
				String baidu = vi.next().toString();
				if(baidu.indexOf("baidu")!=-1){
					count++;
				}
			}
		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new Text("baidu"), new Text(count+""));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"");
		job.setJarByClass(JumpFromBaiDu.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean status = job.waitForCompletion(true);
	}

}
