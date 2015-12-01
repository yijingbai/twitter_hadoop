import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Reducer10 extends Reducer<Text, Text, LongWritable, Text> {
	public void reduce(Text key, Iterable<Text> value, Context context) {
		long communityNum = context.getConfiguration().getLong("communityNum", 0) + 1;
		try {
			context.write(new LongWritable(communityNum), key);
		} catch (Exception e) {
			e.printStackTrace();
		}
		context.getConfiguration().setLong("communityNum", communityNum);
	}
}
