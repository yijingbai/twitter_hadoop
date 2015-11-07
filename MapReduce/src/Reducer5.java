import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Reducer5 extends Reducer<Text, Text, Text, DoubleWritable> {
	public void reduce(Text key, Iterable<Text> value, Context context) {
		
	}
}
