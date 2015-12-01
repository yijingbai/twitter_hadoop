import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MarkableIterator;
import org.apache.hadoop.mapreduce.Reducer;


// key: targetId
// value: sourceId distance weight status pathList adjList

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> value, Context context) {
		Iterator<Text> iter = value.iterator();
		
		HashSet<String> set = new HashSet<String>();
		long minDistance = Long.MAX_VALUE;
		
		while (iter.hasNext()) {
			String s = iter.next().toString();
			long distance = Long.parseLong(s.split(" ")[0]);
			if (distance < minDistance) {
				set.clear();
				set.add(s);
				minDistance = distance;
			} else if (distance == minDistance) {
				set.add(s);
			}
		}
		
		long weight = set.size();
		String[] keyStr = key.toString().split(",");
		for (String s: set) {
			String[] str = s.split(" ");
			try {
				context.write(new Text(keyStr[0]), valueText(keyStr[1], str, minDistance, weight));
			} catch (Exception e) {
				e.printStackTrace();
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
		
		return new Text(sb.toString());
	}
}
