import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context con)
			throws IOException, InterruptedException {
				//get the name of files from path then get the word in each line then write it (word, file name)
		String fileName = ((FileSplit) con.getInputSplit()).getPath().getName();
		String line = value.toString();
		String[] words = line.split(" ");
		for (String word : words) {
			con.write(new Text(word), new Text(fileName));
		}

	}

}
