import java.io.IOException;
import java.util.ArrayList;
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
		//clone iterator???
		//Iterator<Text> iter1 = value.iterator();
		//Iterator<Text> iter2 = value.iterator();
		
		MarkableIterator<Text> iter = new MarkableIterator<Text>(value.iterator());	
		List<String> adjList = new ArrayList<String>();
		
		try {
			iter.mark();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		while (iter.hasNext()) {
			String[] str = iter.next().toString().split(" ");
			if (!str[5].equals("[]")) {
				String adj[] = str[5].substring(1, str[5].length() - 1).split(",");
				for (int i = 0; i < adj.length; i++) {
					adjList.add(adj[i]);
				}
				break;
			}
		}
		
		try {
			iter.reset();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		while (iter.hasNext()) {
			Text text = iter.next();
			String[] str = text.toString().split(" ");
			//if (!key.toString().equals(str[0])) {
				if (str[5].equals("[]")) {
					try {
						context.write(key, valueText(str, adjList));
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					try {
						context.write(key, text);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			//}
		}
	}
	
	public Text valueText(String[] str, List<String> adjList) {
		StringBuilder sb = new StringBuilder();
		
		for (int i = 0; i < str.length - 1; i++) {
			sb.append(str[i]);
			sb.append(' ');
		}
		
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
