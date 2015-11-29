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
		// iterate twice
//		MarkableIterator<Text> iter = new MarkableIterator<Text>(value.iterator());	
//		String adjListStr = "[]";
//		
//		try {
//			iter.mark();
//		} catch (IOException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//		while (iter.hasNext()) {
//			String[] str = iter.next().toString().split(" ");
//			if (!str[5].equals("[]")) {
//				adjListStr = str[5];
//				break;
//			}
//		}
//		
//		try {
//			iter.reset();
//		} catch (IOException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//		while (iter.hasNext()) {
//			Text text = iter.next();
//			String[] str = text.toString().split(" ");
//			//if (!key.toString().equals(str[0])) {
//				if (str[5].equals("[]")) {
//					try {
//						context.write(key, valueText(str, adjListStr));
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//				} else {
//					try {
//						context.write(key, text);
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//				}
//			//}
//		}
		
		
		// using cache
		Iterator<Text> iter = value.iterator();
		
		HashSet<String> cache = new HashSet<String>();
		String adjListStr = "[]";
		
		while (iter.hasNext()) {
			String s = iter.next().toString();
			String[] str = s.split(" ");
			cache.add(s);
			if (!str[5].equals("[]")) {
				adjListStr = str[5];
				break;
			}
		}
		
		while (iter.hasNext()) {
			String s = iter.next().toString();
			String[] str = s.split(" ");
			if (!cache.contains(s)) {
				if (str[5].equals("[]")) {
					try {
						context.write(key, valueText(str, adjListStr));
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					try {
						context.write(key, new Text(s));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		
		for (String s: cache) {
			String[] str = s.split(" ");
			if (str[5].equals("[]")) {
				try {
					context.write(key, valueText(str, adjListStr));
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				try {
					context.write(key, new Text(s));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public Text valueText(String[] str, String adjListStr) {
		StringBuilder sb = new StringBuilder();
		
		for (int i = 0; i < str.length - 1; i++) {
			sb.append(str[i]);
			sb.append(' ');
		}
		
		sb.append(adjListStr);
		
		return new Text(sb.toString());
	}
}
