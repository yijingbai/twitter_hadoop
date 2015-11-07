import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MarkableIterator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

// key: targetId,sourceId
// value: distance weight status pathList adjList

public class Reducer2 extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> value, Context context) {
		MarkableIterator<Text> iter = new MarkableIterator<Text>(value.iterator());	
		long minDistance = Long.MAX_VALUE;
		long count = 0;
		
		try {
			iter.mark();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		while (iter.hasNext()) {
			Text text = iter.next();
			long distance = Long.parseLong(text.toString().split(" ")[0]);
			if (distance < minDistance) {
				minDistance = distance;
				count = 0;
			} else if (distance == minDistance) {
				count++;
			}
		}
		
		try {
			iter.reset();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] keyStr = key.toString().split(",");
		while (iter.hasNext()) {
			Text text = iter.next();
			String[] str = text.toString().split(" ");
			long distance = Long.parseLong(str[0]);
			long weight = Long.parseLong(str[1]);
			if (distance == minDistance) {
				weight += count;
				try {
					context.write(new Text(keyStr[0]), valueText(keyStr[1], str, distance, weight));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public Text valueText(String sourceId, String[] str, long distance, long weight) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(sourceId);
		sb.append(' ');
		sb.append(distance);
		sb.append(' ');
		sb.append(weight);
		sb.append(' ');
		sb.append(str[2]);
		sb.append(' ');
		sb.append(str[3]);
		sb.append(' ');
		sb.append(str[4]);
		
		return new Text(sb.toString());
	}
}
