import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

// key: source,target
// value: 1/weight

public class Reducer2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce(Text key, Iterable<DoubleWritable> value, Context context) {
		Iterator<DoubleWritable> iter = value.iterator();
		double edgeBetweenness = 0;
		
		while (iter.hasNext()) {
			edgeBetweenness += iter.next().get();
		}
		
		try {
			context.write(key, new DoubleWritable(edgeBetweenness));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
