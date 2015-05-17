import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TwitterMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	int len=0;
	int no_of_tweet=0;
	int num_of_hashtags=0;
	int num_of_links=0;
	String myexp="#[a-zA-Z0-9_]";
	String myexp1="http://";
	public void map(LongWritable key,Text value,Context context) throws  IOException, InterruptedException 
	{
		Pattern pattern=Pattern.compile(myexp);
		Pattern pattern1=Pattern.compile(myexp1);
		
		if(key.get()!=0)
		{
		String s=value.toString();		
		String s1=s.replaceAll(",","");
		Matcher matcher=pattern.matcher(s1);
		Matcher matcher1=pattern1.matcher(s1);
		len+=s1.length();
		no_of_tweet+=1;
		while(matcher.find())
			num_of_hashtags+=1;
		while(matcher1.find())
			num_of_links++;
		}
	}
	public void run(Context context) throws IOException, InterruptedException {
		
		setup(context);
		while (context.nextKeyValue()) {
		map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		context.write(new Text("Length of whole tweets"), new IntWritable(len));
		int aver_twe_len=len/no_of_tweet;
		context.write(new Text("Total number of tweets"), new IntWritable(no_of_tweet));
		context.write(new Text("Average tweet Length"), new IntWritable(aver_twe_len));
		context.write(new Text("Number of hashtags"), new IntWritable(num_of_hashtags));
		context.write(new Text("Number of Links"), new IntWritable(num_of_links));
		context.write(new Text("Users c"), new IntWritable(num_of_links));
		cleanup(context); 
		}
}
	
