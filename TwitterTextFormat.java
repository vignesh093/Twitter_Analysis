import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class TwitterTextFormat extends TextInputFormat {
	protected boolean isSplitable(JobContext context,Path filename)
	{
		return false;
	}

}
