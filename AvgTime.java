package SessionProject;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
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

public class AvgTime {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			// 正则匹配
			String pattern = "^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}) [^ ]+ [^ ]+ \\[([^ ]+ [^ ]+)\\] \"[^ ]+ ([^ ]+)";
			Pattern r = Pattern.compile(pattern);
			Matcher m = r.matcher(value.toString());
			String showPattern = "^(/show/\\d+)($|\\?.*)";
			Pattern rShow = Pattern.compile(showPattern);
			if (m.find()){
				String ip = m.group(1);
				String time = m.group(2);
				String url = m.group(3);
				Matcher mShow = rShow.matcher(url);
				if (mShow.find()){
					context.write(new Text(ip), new Text(time +"\t"+ mShow.group(1)));
				}
			}
		}	
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		int count = 0;
		long avg = 0;
		long sumDiff = 0;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			count++;
			List<Date_sort> list =new ArrayList<Date_sort>();
			SimpleDateFormat regularFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US);
			Iterator<Text> vi = values.iterator();
			while(vi.hasNext()){
				Text s = vi.next();
				String line = s.toString();
				String[] tokens = line.split("\t");
				Date date = null;
				try {
					date = regularFormat.parse(tokens[0]);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Date_sort ds = new Date_sort();
				ds.setDate(date);
				ds.setUrl(tokens[1]);
				list.add(ds);
			}
			Collections.sort(list,new Comparator<Date_sort>(){

				public int compare(Date_sort o1, Date_sort o2) {
					// TODO Auto-generated method stub
					if((o2.getDate().getTime()-o1.getDate().getTime())>0)
						return -1;
					else if ((o2.getDate().getTime()-o1.getDate().getTime())<0)
						return 1;
					else 
						return 0;
				}
			});
			
			long diff = list.get(list.size()-1).getDate().getTime()-list.get(0).getDate().getTime();
			sumDiff+=diff;
		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			avg = (sumDiff/count)/60000;
			context.write(new Text(""), new Text(avg+"s"));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"");
		job.setJarByClass(AvgTime.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean status = job.waitForCompletion(true);
	}
}
class Date_sort{
	private Date date;
	private String url;
	public Date_sort(Date date, String url) {
		super();
		this.date = date;
		this.url = url;
	}
	public Date_sort() {
		super();
	}
	public Date getDate() {
		return date;
	}
	public void setDate(Date date) {
		this.date = date;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
}
