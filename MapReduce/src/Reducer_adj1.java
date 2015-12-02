import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Reducer_adj1 extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> value, Context context) {
		Iterator<Text> iter = value.iterator();
		
		StringBuilder adjListSb = new StringBuilder();
		
		while (iter.hasNext()) {
			String followingId = iter.next().toString();
			adjListSb.append(followingId);
			adjListSb.append(',');
		}
		adjListSb.deleteCharAt(adjListSb.length() - 1);
		
		try {
			context.write(key, new Text(adjListSb.toString()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
