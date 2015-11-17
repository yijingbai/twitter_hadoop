import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

// key: user1
// value: user2
// key: targetId
// value: sourceId distance weight status pathList adjList

public class Reducer0 extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> value, Context context) {
		Iterator<Text> iter = value.iterator();
		
		String targetId = key.toString();
		List<String> adjList = new ArrayList<String>();
		
		while (iter.hasNext()) {
			String followingId = iter.next().toString();
			adjList.add(followingId);
		}
		
		try {
			context.write(key, valueText(targetId, adjList));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public Text valueText(String targetId, List<String> adjList) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(targetId);
		sb.append(' ');
		sb.append(0);
		sb.append(' ');
		sb.append(1);
		sb.append(' ');
		sb.append("active");
		sb.append(' ');
		sb.append("[]");
		sb.append(' ');
		sb.append('[');
		for (int i = 0; i < adjList.size(); i++) {
			sb.append(adjList.get(i));
			if (i < adjList.size() - 1)
				sb.append(',');
		}
		sb.append(']');
		
		return new Text(sb.toString());
	}
}
