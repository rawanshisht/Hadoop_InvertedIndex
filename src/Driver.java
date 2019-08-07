
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver {
	public static void main(String[] args) throws Exception {
		//we get the input path and identify the driver, mapper and reducer, the ouput will be the word and count in all files and names of the files.
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

		Path input = new Path(files[0]);
		Path output = new Path(files[1]);

		Job job = new Job(c, "Task");

		job.setJarByClass(Driver.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
