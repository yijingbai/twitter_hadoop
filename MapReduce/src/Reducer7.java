import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Reducer7 extends Reducer<Text, Text, LongWritable, Text> {
	public void reduce(Text key, Iterable<Text> value, Context context) {
		Driver.communityNum++;
		try {
			context.write(new LongWritable(Driver.communityNum), key);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
