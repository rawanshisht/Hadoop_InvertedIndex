import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context con)
			throws IOException, InterruptedException {
		HashMap<String, Integer> m = new HashMap<String, Integer>();
		int count = 0;
		for (Text t : values) {
			//for each value we have we count it if it exists in different file then put the word and value after we increase it, else it appears just once in all files.
			String str = t.toString();
			if (m != null && m.get(str) != null) {
				count = (int) m.get(str);
				m.put(str, ++count);
			} else {
				m.put(str, 1);
			}
		}
		con.write(key, new Text(m.toString()));
	}

}
